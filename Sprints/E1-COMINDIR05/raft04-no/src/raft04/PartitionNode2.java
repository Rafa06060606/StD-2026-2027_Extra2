package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class PartitionNode2 {

    // ---- identificação do nó ----
    private final int k;          // partição de dados (p0, p1, p2)
    private final int i;          // número do nó dentro do grupo (1, 2 ou 3)
    private final int n;          // número total de partições (dimensão do vetor V)
    private final String host;
    private final int port;       // porto TCP de recuperação deste nó

    // ---- componentes ----
    private final Partitioner partitioner;  // sensor -> partição
    private final RecordFile file;          // no-p<k>-n<i>.txt: os registos deste nó
    private final RaftState raft;           // termo, voto e papel (vem da ficha)
    private final IpfsPubSub publisher;     // só publica
    private final IpfsPubSub subscriber;    // só subscreve (cada subscribe() fica preso numa thread)
    private final Path eventsFile;          // eventos-p<k>-n<i>.txt
    private final Path hbFile;              // hb-p<k>-n<i>.txt: uma linha por lote publicado
    private final Path truncFile;           // truncados-p<k>-n<i>.txt: os registos descartados (para o RaftCheck)
    private final Object eventLock = new Object();   // só protege a escrita dos eventos

    // ---- estado dos registos (protegido por synchronized (this)) ----
    private long seq;          // último seq que o nó tem
    private long lastTerm;     // termo desse último registo
    private long lamport;      // L
    private VectorClock clock; // V
    private long hb = 0;                                       // contador de heartbeats (só líder)
    private List<SensorRecord> pendentes = new ArrayList<>();  // registos à espera do próximo lote (só líder)
    private String leaderHost;                                 // endereço do líder, aprendido nos lotes
    private int leaderPort = -1;
    private ScheduledExecutorService scheduler;                // envia os lotes (só enquanto for líder)
    private volatile long syncedTerm = 0;                      // termo do líder com quem o meu ficheiro já foi confirmado

    public PartitionNode2(int k, int i, int n, String host, String api) throws IOException {
        if (n <= 0 || k < 0 || k >= n) throw new IllegalArgumentException("Partição inválida: k=" + k + " N=" + n);
        if (i < 1 || i > Wire.GRUPO)   throw new IllegalArgumentException("Nó inválido: i=" + i);

        this.k = k;
        this.i = i;
        this.n = n;
        this.host = host;
        this.port = Wire.nodePort(k, i);
        this.partitioner = new Partitioner(n);
        this.file = new RecordFile("no-p" + k + "-n" + i + ".txt");
        this.eventsFile = Paths.get("eventos-p" + k + "-n" + i + ".txt");
        this.hbFile = Paths.get("hb-p" + k + "-n" + i + ".txt");
        this.truncFile = Paths.get("truncados-p" + k + "-n" + i + ".txt");
        this.publisher = new IpfsPubSub(api);
        this.subscriber = new IpfsPubSub(api);

        loadFromFile();   // seq, termo do último registo, L e V

        // o RaftState lê o estado-p<k>-n<i>.txt (termo e voto) e arranca como seguidor
        this.raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Paths.get("estado-p" + k + "-n" + i + ".txt"), this::logEvent);

        log("ARRANQUE", "no=n" + i + ";seq=" + seq + ";ultimoTermo=" + lastTerm
                + ";termo=" + raft.term() + ";porto=" + port);
    }

    // seq, termo, L e V vêm da última linha do ficheiro (ou zero se não existir)
    // Usado no arranque e (camada 3) ao passar a líder.
    private void loadFromFile() throws IOException {
        SensorRecord last = file.last();
        if (last == null) {
            seq = 0; lastTerm = 0; lamport = 0;
            clock = new VectorClock(n);
            return;
        }
        if (last.getVector().get(k) != last.getSeq())
            throw new IllegalStateException("Ficheiro inconsistente: V[k] != seq em " + last.toLine());
        seq = last.getSeq();
        lastTerm = last.getTerm();
        lamport = last.getLamport();
        clock = last.getVector().copy();
    }

    // ---------- arranque ----------

    public void start() throws IOException {
        startTcpServer();
        startElectionChecker();
        subscribeEmThread(Wire.topicElection(k), this::onElectionMessage);
        subscribeEmThread(Wire.topicIn(k), this::onInput);        // leituras: só o líder as aceita
        subscribeEmThread(Wire.topicData(k), this::onBatchMessage); // lotes do líder
    }

    // ---------- eleição ----------

    // thread que, a cada VERIFICACAO_MS, pergunta ao RaftState se o prazo expirou
    private void startElectionChecker() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    checkElection();
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    log("ERRO", "verificacao=" + e.getMessage());
                }
            }
        });
        t.setName("Eleicao-p" + k + "-n" + i);
        t.setDaemon(true);
        t.start();
    }

    private void checkElection() {
        long term = raft.tick();                  // devolve o termo da candidatura, ou -1
        if (term == -1 || Wire.isolated(k, i)) return;

        long myLastSeq, myLastTerm;
        synchronized (this) {
            // entretanto pode ter chegado um lote ou um termo superior: volta a confirmar
            if (raft.role() != RaftState.Role.CANDIDATO || raft.term() != term) return;
            myLastSeq = seq;
            myLastTerm = lastTerm;
        }
        publishElection(new VoteRequest(term, i, myLastSeq, myLastTerm).encode());
    }

    // mensagens do tópico de eleição: PEDIDO_VOTO ou VOTO
    private void onElectionMessage(String msg) {
        if (Wire.isolated(k, i)) return;          // nó isolado não processa mensagens
        try {
            String type = Wire.type(msg);
            if (type.equals("PEDIDO_VOTO"))  onVoteRequest(VoteRequest.decode(msg));
            else if (type.equals("VOTO"))    onVote(VoteReply.decode(msg));
            else                             log("IGNORADO", "tipo=" + type);
        } catch (Exception e) {
            log("ERRO", "eleicao=" + e.getMessage());
        }
    }

    // alguém pede o meu voto
    private void onVoteRequest(VoteRequest r) {
        checkId(r.candidate, r.term);
        if (r.candidate == i) return;             // ignora a minha própria publicação

        RaftState.Vote vote;
        synchronized (this) {                     // 1.º lock: o monitor do nó
            boolean wasLeader = raft.termIfLeader() != -1;
            vote = raft.onVoteRequest(r.term, r.candidate, r.lastSeq, r.lastTerm, seq, lastTerm);  // 2.º lock: RaftState
            if (wasLeader && raft.termIfLeader() == -1) stopLeaderLocked();
        }
        // responde sempre, mesmo quando recusa, e fora do monitor (publicar é rede)
        publishElection(new VoteReply(vote.term, i, r.candidate, vote.granted).encode());
    }

    // chegou um voto
    private void onVote(VoteReply r) throws IOException {
        checkId(r.voter, r.term);
        checkId(r.candidate, r.term);
        if (r.candidate != i || r.voter == i) return;   // só interessam votos dirigidos a mim

        synchronized (this) {
            boolean wasLeader = raft.termIfLeader() != -1;
            boolean elected = raft.onVote(r.term, r.voter, r.candidate, r.granted);
            if (wasLeader && raft.termIfLeader() == -1) stopLeaderLocked();
            if (elected) becomeLeaderLocked();
        }
    }

    // recusa identificadores e termos impossíveis (mensagem forjada ou com erro)
    private void checkId(int node, long term) {
        if (node < 1 || node > Wire.GRUPO) throw new IllegalArgumentException("nó fora do grupo: " + node);
        if (term < 1) throw new IllegalArgumentException("termo inválido: " + term);
    }

    private void publishElection(String msg) {
        if (Wire.isolated(k, i)) return;          // nó isolado não publica
        try {
            publisher.publish(Wire.topicElection(k), msg);
        } catch (Exception e) {
            log("ERRO", "publicar_eleicao=" + e);
        }
    }

    // ---------- papel de líder ----------

    // chamado dentro do monitor, no instante em que o RaftState me elege
    private void becomeLeaderLocked() throws IOException {
        loadFromFile();                       // seq, L e V a partir do último registo do ficheiro
        hb = 0;
        pendentes = new ArrayList<>();
        System.out.println("Passei a líder: termo=" + raft.term() + " seq=" + seq + " L=" + lamport + " V=" + clock);

        stopLeaderLocked();                   // se houver um agendador antigo, cancela-o
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Lider-p" + k + "-n" + i);
            t.setDaemon(true);
            return t;
        });
        // enviarLote() corre já e depois a cada PERIODO_HB_MS
        scheduler.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    // chamado dentro do monitor, quando o RaftState me demitiu
    private void stopLeaderLocked() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    // uma leitura IN publicada pelo produtor: IN;<sensor>;<temperatura>;<L>;<V>
    private void onInput(String msg) {
        try {
            String[] parts = msg.trim().split(";");
            if (parts.length != 5 || !parts[0].equals("IN") || parts[1].isEmpty()) {
                log("ERRO", "IN_mal_formado=" + msg);
                return;
            }
            String sensor = parts[1];
            double temp = Double.parseDouble(parts[2]);
            long lMsg = Long.parseLong(parts[3]);
            VectorClock vMsg = VectorClock.parse(parts[4], n);
            if (Double.isNaN(temp) || Double.isInfinite(temp)) throw new IllegalArgumentException("temperatura inválida");

            if (partitioner.of(sensor) != k) {
                log("IGNORADO", "IN;sensor=" + sensor + ";particao_errada");
                return;
            }

            // UMA secção sincronizada: termo, L, V, seq, escrita e pendentes.
            // Se me demitirem a meio, ou ganho este monitor antes (e a leitura leva o termo antigo,
            // ainda válido) ou depois (termIfLeader() dá -1 e a leitura é ignorada).
            synchronized (this) {
                long t = raft.termIfLeader();
                if (t == -1) {
                    log("IGNORADO", "IN;sensor=" + sensor + ";sem_lider");
                    return;
                }
                // tudo calculado sobre cópias: se algo falhar, o estado do nó fica intacto
                long newL = Math.max(lamport, lMsg) + 1;
                VectorClock newV = clock.copy();
                newV.merge(vMsg);
                newV.tick(k);
                long newSeq = seq + 1;
                if (newV.get(k) != newSeq)
                    throw new IllegalStateException("Invariante violada: V[k]=" + newV.get(k) + " != seq=" + newSeq);

                SensorRecord r = SensorRecord.now(newSeq, t, sensor, temp, newL, newV);
                file.append(r);                       // se falhar, as linhas seguintes não correm

                seq = newSeq;
                lastTerm = t;
                lamport = newL;
                clock = newV;
                pendentes.add(r);                     // fica à espera do próximo lote
                System.out.println("IN aceite: seq=" + seq + " termo=" + t + " L=" + lamport + " V=" + clock);
            }
        } catch (Exception e) {
            log("ERRO", "IN=" + msg + ";" + e);
        }
    }

    // heartbeat + lote: corre a cada PERIODO_HB_MS enquanto for líder
    private void enviarLote() {
        if (Wire.isolated(k, i)) return;              // isolado: não publica; os pendentes ficam
        try {
            Batch batch;
            synchronized (this) {
                long t = raft.termIfLeader();
                if (t == -1) {                        // já não sou líder: cancela e termina
                    stopLeaderLocked();
                    return;
                }
                List<SensorRecord> lote = pendentes;
                pendentes = new ArrayList<>();
                batch = new Batch(t, i, host, port, ++hb, seq, lastTerm, lote);
            }
            // fora do monitor: publicar é rede e não deve bloquear os IN nem os votos.
            // Se falhar, estes registos não são reenviados; os seguidores recuperam-nos por TCP
            // porque o cabeçalho traz ultimoSeq.
            String msg = batch.encode();
            publisher.publish(Wire.topicData(k), msg);

            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            Files.writeString(hbFile,
                    batch.term + ";" + batch.hb + ";" + System.currentTimeMillis() + ";" + batch.lastSeq
                            + ";" + batch.lastTerm + ";" + batch.n + ";" + bytes + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Throwable t) {
            // uma exceção não tratada cancelaria as execuções seguintes do agendador
            log("ERRO", "enviarLote=" + t);
        }
    }

    // ---------- papel de seguidor ----------

    // um lote do tópico raft04-p<k>
    private void onBatchMessage(String msg) {
        if (Wire.isolated(k, i)) return;              // nó isolado não processa mensagens
        try {
            Batch b = Batch.decode(msg, n);
            checkId(b.leader, b.term);
            if (b.port != Wire.nodePort(k, b.leader)) throw new IllegalArgumentException("porto incoerente: " + b.port);
            if (b.leader == i) return;                // ignora o que eu próprio publiquei

            synchronized (this) {
                boolean wasLeader = raft.termIfLeader() != -1;
                boolean accepted = raft.onBatch(b.term, b.leader);   // renova o prazo; NOVO_LIDER; adota termo
                if (wasLeader && raft.termIfLeader() == -1) stopLeaderLocked();
                if (!accepted) {
                    log("IGNORADO", "LOTE;termo=" + b.term + ";lider=n" + b.leader);
                    return;
                }
                leaderHost = b.address;
                leaderPort = b.port;
                String intervalo = b.records.isEmpty() ? "-"
                        : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq();
                log("RECEBIDO", "termo=" + b.term + ";lider=n" + b.leader + ";hb=" + b.hb
                        + ";ultimoSeq=" + b.lastSeq + ";n=" + b.n + ";seq=" + intervalo);
            }
            applyBatch(b);   // fora do monitor: pode precisar de TCP
        } catch (Exception e) {
            log("ERRO", "lote=" + e);
        }
    }

    // aplica os registos de um lote aceite. Corre sempre na thread do tópico raft04-p<k>,
    // por isso dois lotes nunca são aplicados ao mesmo tempo.
    private void applyBatch(Batch b) throws IOException {
        // 1.º lote de um termo novo: antes de acrescentar o que quer que seja, confirma que o meu
        // ficheiro é um prefixo do do líder (e, se não for, trunca-o). Sem isto, um nó que regressa
        // atrasado acrescentava os registos novos por cima de registos antigos divergentes.
        if (syncedTerm != b.term) {
            if (!reconcile(b)) return;       // líder inacessível, etc.: tenta no lote seguinte
            syncedTerm = b.term;
        }

        for (SensorRecord r : b.records) {
            long mySeq;
            synchronized (this) { mySeq = seq; }

            if (r.getSeq() <= mySeq) {                       // seq que já tenho
                if (termAt(r.getSeq()) == r.getTerm()) continue;   // mesmo termo: duplicado
                resolveDivergence(b);                        // outro termo: divergência
                return;
            }
            if (r.getSeq() > mySeq + 1)                      // lacuna: recupera o que falta por TCP
                recoverRange(mySeq + 1, r.getSeq() - 1, b.term);

            if (!appendIfNext(r, b.term)) {                  // seq = último + 1: acrescenta
                log("ERRO", "lacuna_na_recuperacao;seq=" + r.getSeq());
                return;
            }
        }

        // fim do lote: compara o meu último registo com ultimoSeq e ultimoTermo do líder
        long s, t;
        synchronized (this) { s = seq; t = lastTerm; }
        if (s < b.lastSeq) {                                 // tenho menos: recupera o resto
            recoverRange(s + 1, b.lastSeq, b.term);
            synchronized (this) { s = seq; t = lastTerm; }
        }
        if (s == b.lastSeq && t != b.lastTerm) {
            resolveDivergence(b);                            // mesmo seq, outro termo
        } else if (s > b.lastSeq && t < b.term) {
            resolveDivergence(b);                            // registos de um termo antigo para além do líder
        }
        // s > ultimoSeq com t == b.term: lote atrasado (chegou fora de ordem); não é divergência
    }

    // acrescenta r ao ficheiro se for exatamente o seguinte; valida-o antes
    private synchronized boolean appendIfNext(SensorRecord r, long batchTerm) throws IOException {
        if (raft.termIfLeader() != -1) return false;          // fui eleito entretanto: não aplico lotes
        if (r.getSeq() != seq + 1) return false;
        if (r.getTerm() < lastTerm || r.getTerm() > batchTerm)
            throw new IllegalArgumentException("termo incoerente no seq " + r.getSeq() + ": " + r.getTerm());
        if (r.getVector().get(k) != r.getSeq())
            throw new IllegalArgumentException("V[k] != seq no seq " + r.getSeq());

        file.append(r);
        seq = r.getSeq();
        lastTerm = r.getTerm();
        lamport = r.getLamport();
        clock = r.getVector().copy();
        System.out.println("Aplicado: " + r.toLine());
        return true;
    }

    // termo do meu registo com este seq, ou -1 se não o tiver
    private long termAt(long s) throws IOException {
        List<SensorRecord> l = file.range(s, s);
        return l.isEmpty() ? -1 : l.get(0).getTerm();
    }

    // ---------- recuperação por TCP (como cliente) ----------

    // pede ao líder os registos from..to e acrescenta-os pela ordem
    private void recoverRange(long from, long to, long batchTerm) throws IOException {
        if (from > to) return;
        log("RECUPERACAO", "de=" + from + ";ate=" + to);
        for (SensorRecord r : fetchFromLeader(from, to)) {
            if (!appendIfNext(r, batchTerm)) break;           // deixou de bater certo: pára
        }
    }

    // só rede: devolve os registos que o líder enviou; não mexe no estado do nó nem usa o monitor
    private List<SensorRecord> fetchFromLeader(long from, long to) throws IOException {
        String h;
        int p;
        synchronized (this) { h = leaderHost; p = leaderPort; }
        if (h == null || p < 0) throw new IOException("líder desconhecido");
        if (Wire.isolated(k, i)) throw new IOException("nó isolado");

        List<SensorRecord> out = new ArrayList<>();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(h, p), Wire.T_TCP_MS);
            socket.setSoTimeout(Wire.T_TCP_MS);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter w = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

            w.println("PEDIDO " + from + " " + to);
            String line;
            boolean fim = false;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.equals("FIM")) { fim = true; break; }
                if (line.startsWith("ERRO")) throw new IOException(line);
                out.add(SensorRecord.fromLine(line, n));
            }
            if (!fim) throw new IOException("ligação terminada sem FIM");
        }
        return out;
    }

    // ---------- divergência e truncagem ----------

    // divergência inesperada dentro de um termo já sincronizado: volta a reconciliar
    private void resolveDivergence(Batch b) throws IOException {
        log("ERRO", "divergencia_inesperada;meuSeq=" + seq + ";meuTermo=" + lastTerm
                + ";liderSeq=" + b.lastSeq + ";liderTermo=" + b.lastTerm);
        syncedTerm = 0;
        if (reconcile(b)) syncedTerm = b.term;
    }

    // Garante que o meu ficheiro é um prefixo do ficheiro do líder, truncando o que divergir.
    // Devolve false se não foi possível (por exemplo, líder inacessível): tenta-se no lote seguinte.
    // Acrescenta a truncados-p<k>-n<i>.txt as linhas dos registos que vão ser descartados.
    // Não faz parte do protocolo: é só prova para o RaftCheck saber QUE registos se perderam.
    private void guardarDescartados(List<SensorRecord> descartados) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (SensorRecord r : descartados)
            sb.append(System.currentTimeMillis()).append(';').append(r.toLine()).append(System.lineSeparator());
        Files.writeString(truncFile, sb.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private boolean reconcile(Batch b) throws IOException {
        List<SensorRecord> loc = file.all();
        int end = loc.size();
        if (end == 0) return true;                                      // ficheiro vazio: nada a verificar
        for (int j = 0; j < end; j++)
            if (loc.get(j).getSeq() != j + 1) throw new IllegalStateException("ficheiro com lacunas no seq " + (j + 1));

        SensorRecord tail = loc.get(end - 1);
        if (tail.getSeq() == b.lastSeq && tail.getTerm() == b.lastTerm) return true;   // mesmo último registo: iguais

        int start = blockStartIndex(loc, end - 1);                      // 1.º registo do meu último bloco de termo
        while (true) {
            long fromSeq = loc.get(start).getSeq();
            List<SensorRecord> lead = fetchFromLeader(fromSeq, Long.MAX_VALUE);   // o que o líder tem a partir daí
            boolean firstMatches = !lead.isEmpty() && lead.get(0).toLine().equals(loc.get(start).toLine());
            if (!firstMatches && start > 0) {                           // este bloco inteiro diverge: recua um bloco de termo
                start = blockStartIndex(loc, start - 1);
                continue;
            }

            // os ficheiros coincidem até ao primeiro registo diferente (ou até ao fim de um deles)
            int m = 0;
            while (start + m < end && m < lead.size()
                    && lead.get(m).toLine().equals(loc.get(start + m).toLine())) m++;
            long divSeq = fromSeq + m;                                  // 1.º seq a substituir
            List<SensorRecord> repl = new ArrayList<>(lead.subList(m, lead.size()));
            int discarded = end - (int) (divSeq - 1);                   // registos meus que vão desaparecer
            if (discarded == 0 && repl.isEmpty()) return true;          // já estava tudo igual

            // valida o que veio do líder antes de mexer no ficheiro
            long prevTerm = divSeq > 1 ? loc.get((int) divSeq - 2).getTerm() : 0;
            long expected = divSeq;
            for (SensorRecord r : repl) {
                if (r.getSeq() != expected || r.getTerm() < prevTerm || r.getTerm() > b.term
                        || r.getVector().get(k) != r.getSeq())
                    throw new IllegalArgumentException("registo inválido do líder no seq " + r.getSeq());
                prevTerm = r.getTerm();
                expected++;
            }

            LinkedHashSet<Long> terms = new LinkedHashSet<>();          // termos dos registos descartados
            for (int j = (int) divSeq - 1; j < end; j++) terms.add(loc.get(j).getTerm());
            StringBuilder lista = new StringBuilder();
            for (long t : terms) lista.append(lista.length() == 0 ? "" : ",").append(t);

            synchronized (this) {                                       // só aqui se mexe no ficheiro e no estado
                if (raft.termIfLeader() != -1 || seq != end) {          // fui eleito, ou o ficheiro mudou entretanto
                    log("ERRO", "reconciliacao_abortada");
                    return false;
                }
                guardarDescartados(loc.subList((int) divSeq - 1, end));   // antes de apagar, guarda-os à parte
                file.replaceFrom(divSeq, repl);                         // escrita atómica (temporário + ATOMIC_MOVE)
                loadFromFile();                                         // seq, termo, L e V a partir do novo último registo
            }
            if (discarded > 0) log("TRUNCADO", "de=" + divSeq + ";descartados=" + discarded + ";termos=" + lista);
            else if (!repl.isEmpty()) log("RECUPERACAO", "de=" + divSeq + ";ate=" + (divSeq + repl.size() - 1));
            return true;
        }
    }

    // índice do primeiro registo do bloco de termo a que pertence o registo idx
    private static int blockStartIndex(List<SensorRecord> loc, int idx) {
        long term = loc.get(idx).getTerm();
        while (idx > 0 && loc.get(idx - 1).getTerm() == term) idx--;
        return idx;
    }

    // ---------- subscrições ----------

    // subscribe() não volta enquanto a ligação durar: cada tópico fica numa thread própria.
    // Se a ligação cair (ou chegar lixo), volta a subscrever passado um tempo.
    private void subscribeEmThread(String topic, Consumer<String> handler) {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    subscriber.subscribe(topic, handler);
                    log("ERRO", "subscricao_fechada=" + topic);
                } catch (Exception e) {
                    log("ERRO", "subscricao=" + topic + ";" + e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.setName("Sub-" + topic);
        t.setDaemon(true);
        t.start();
    }

    // ---------- servidor TCP de recuperação (serve registos a qualquer nó) ----------

    private void startTcpServer() throws IOException {
        // aberto aqui, e não dentro da thread: se o porto estiver ocupado, o nó nem arranca
        ServerSocket server = new ServerSocket(port);

        Thread acceptor = new Thread(() -> {
            while (true) {
                try {
                    Socket socket = server.accept();                 // espera um cliente
                    Thread handler = new Thread(() -> handleRecovery(socket));
                    handler.setDaemon(true);
                    handler.start();                                 // um cliente por thread
                } catch (IOException e) {
                    log("ERRO", "servidorTCP=" + e.getMessage());
                    return;
                }
            }
        });
        acceptor.setName("TCP-p" + k + "-n" + i);
        acceptor.start();   // esta thread (não daemon) é a que mantém o processo vivo
    }

    // atende um cliente: PEDIDO <de> <ate>  ->  linhas de registo e FIM
    private void handleRecovery(Socket socket) {
        try (Socket s = socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(
                     new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

            if (Wire.isolated(k, i)) return;          // nó isolado: fecha sem responder
            s.setSoTimeout(Wire.T_TCP_MS);            // cliente calado não prende a thread

            String request = in.readLine();
            if (request == null) return;
            String[] p = request.trim().split("\\s+");
            if (p.length != 3 || !p[0].equals("PEDIDO")) {
                out.println("ERRO Pedido mal formado");
                return;
            }

            long from, to;
            try {
                from = Long.parseLong(p[1]);
                to = Long.parseLong(p[2]);
            } catch (NumberFormatException e) {
                out.println("ERRO Valores numericos invalidos");
                return;
            }
            if (from < 1 || from > to) {
                out.println("ERRO Intervalo invalido");
                return;
            }

            List<SensorRecord> records = file.range(from, to);   // só devolve o que existe
            for (SensorRecord r : records) out.println(r.toLine());
            out.println("FIM");
            System.out.println("TCP: servidos " + records.size() + " registos (" + from + ".." + to + ")");
        } catch (Exception e) {
            log("ERRO", "TCP_recuperacao=" + e.getMessage());
        }
    }

    // ---------- eventos ----------

    // log("CANDIDATO", "termo=2")  ->  CANDIDATO;p1;termo=2
    private void log(String event, String detail) {
        logEvent(event + ";p" + k + ";" + detail);
    }

    // recebe "EVENTO;p<k>;detalhe" (também do RaftState) e escreve instante;evento;p<k>;detalhe
    private void logEvent(String message) {
        System.out.println(message);
        String[] f = message.split(";", 3);   // evento, partição, detalhe
        String line = System.currentTimeMillis() + ";" + f[0] + ";" + f[1] + ";" + (f.length > 2 ? f[2] : "")
                + System.lineSeparator();
        synchronized (eventLock) {
            try {
                Files.writeString(eventsFile, line, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Erro ao escrever eventos: " + e.getMessage());
            }
        }
    }

    // ---------- main ----------

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: java raft04.PartitionNode2 <k> <i> [N] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        PartitionNode2 node = new PartitionNode2(k, i, n, "127.0.0.1", api);
        node.start();
        System.out.println("Nó pronto: papel=" + node.raft.role() + " termo=" + node.raft.term()
                + " seq=" + node.seq + " | TCP em " + node.host + ":" + node.port);
    }
}