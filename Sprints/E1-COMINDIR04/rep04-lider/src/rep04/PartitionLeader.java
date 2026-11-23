package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {
    private final int k;
    private final int n;
    private final int port;
    private final String host;
    private final Partitioner partitioner;
    private final RecordFile file;
    private final Path hbFile;
    private final IpfsPubSub publisher;
    private final long inc;   // encarnação: instante de arranque em ms

    // estado partilhado (protegido por synchronized (this))
    private long seq;
    private long lamport;
    private final VectorClock clock;
    private long hb = 0;
    private List<SensorRecord> pendentes = new ArrayList<>();

    public PartitionLeader(int k, int n, String host, String api) throws Exception {
        this.k = k;
        this.n = n;
        this.host = host;
        this.port = Wire.PORTO_BASE + k;
        this.partitioner = new Partitioner(n);
        this.file = new RecordFile("lider-p" + k + ".txt");
        this.hbFile = Paths.get("hb-p" + k + ".txt");
        this.publisher = new IpfsPubSub(api);
        this.inc = System.currentTimeMillis();

        // arranque: seq, L e V vêm da última linha do ficheiro (ou zero se não existir)
        SensorRecord last = file.last();
        if (last == null) {
            this.seq = 0;
            this.lamport = 0;
            this.clock = new VectorClock(n);
        } else {
            int dim = last.getVector().toString().split(",").length;
            if (dim != n) {
                throw new IllegalStateException("O último registo de lider-p" + k + ".txt tem vetor de dimensão "
                        + dim + ", mas N = " + n);
            }
            if (last.getVector().get(k) != last.getSeq()) {
                throw new IllegalStateException("Ficheiro inconsistente: V[k] != seq na última linha: " + last.toLine());
            }
            this.seq = last.getSeq();
            this.lamport = last.getLamport();
            this.clock = last.getVector().copy();
        }

        IpfsPubSub pubsub = new IpfsPubSub(api);

        // servidor TCP de recuperação (inalterado)
        new Thread(this::startTcpServer).start();

        // heartbeat + lote: no arranque e depois a cada PERIODO_HB_MS
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        System.out.println("Líder p" + k + " (N=" + n + ") inc=" + inc + ": seq = " + seq + ", L = " + lamport
                + ", V = " + clock + ", subscreve " + Wire.topicIn(k) + ", à escuta em " + host + ":" + port
                + ", período = " + Wire.PERIODO_HB_MS + " ms");

        pubsub.subscribe(Wire.topicIn(k), msg -> {
            try {
                String trimmedMsg = msg != null ? msg.trim() : "";
                String[] parts = trimmedMsg.split(";");

                // IN;<sensor>;<temperatura>;<L>;<V>
                if (parts.length != 5 || !parts[0].equals("IN") || parts[1].isEmpty()) {
                    System.out.println("REJEITADO: " + trimmedMsg);
                    return;
                }

                String sensor = parts[1];
                double temp;
                long lMsg;
                VectorClock vMsg;
                try {
                    temp = Double.parseDouble(parts[2]);
                    lMsg = Long.parseLong(parts[3]);
                    vMsg = VectorClock.parse(parts[4], n);
                } catch (RuntimeException e) {
                    System.out.println("REJEITADO: " + trimmedMsg + " (" + e.getMessage() + ")");
                    return;
                }

                int expectedK = partitioner.of(sensor);
                if (expectedK != k) {
                    System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + expectedK);
                    return;
                }

                SensorRecord r;
                // Uma única secção sincronizada: Lamport, vetor, seq, verificação, append e pendentes
                synchronized (this) {
                    lamport = Math.max(lamport, lMsg) + 1;
                    clock.merge(vMsg);
                    clock.tick(k);
                    seq++;
                    if (clock.get(k) != seq) {
                        throw new IllegalStateException("Invariante violada: V[k]=" + clock.get(k) + " != seq=" + seq);
                    }
                    r = SensorRecord.now(seq, sensor, temp, lamport, clock.copy());
                    file.append(r);
                    pendentes.add(r);   // fica à espera do próximo lote
                }

                System.out.println("EVENTO: seq=" + r.getSeq() + " L=" + r.getLamport() + " V=" + r.getVector()
                        + " (pendente)");

            } catch (Exception e) {
                System.out.println("REJEITADO: " + msg + " (Erro: " + e.getMessage() + ")");
            }
        });
    }

    // Publica o lote (= heartbeat) com os registos acumulados desde o lote anterior
    private void enviarLote() {
        try {
            List<SensorRecord> lote;
            long ultimoSeq;
            long hbAtual;
            // mesmo monitor da atribuição de seq: o lote e ultimoSeq são coerentes
            synchronized (this) {
                lote = pendentes;
                pendentes = new ArrayList<>();
                ultimoSeq = seq;
                hbAtual = ++hb;
            }

            // fora da secção sincronizada: a publicação pode demorar e não deve bloquear os IN
            Batch b = new Batch(host, port, inc, hbAtual, ultimoSeq, lote);
            String msg = b.encode();
            try {
                publisher.publish(Wire.topicData(k), msg);
            } catch (Exception e) {
                System.out.println("ERRO ao publicar lote hb=" + hbAtual + " (" + lote.size()
                        + " registos não são reenviados): " + e.getMessage());
                return;
            }

            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            Files.writeString(hbFile,
                    inc + ";" + hbAtual + ";" + System.currentTimeMillis() + ";" + ultimoSeq + ";" + lote.size()
                            + ";" + bytes + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("HEARTBEAT: hb=" + hbAtual + " n=" + lote.size() + " ultimoSeq=" + ultimoSeq);
        } catch (Throwable t) {
            // uma exceção não tratada cancelaria as execuções seguintes do agendador
            System.out.println("ERRO em enviarLote: " + t);
        }
    }

    private void startTcpServer() {
        try (ServerSocket server = new ServerSocket(port)) {
            while (true) {
                Socket socket = server.accept();
                handleRecovery(socket);
            }
        } catch (IOException e) {
            System.out.println("Erro no servidor TCP p" + k + ": " + e.getMessage());
        }
    }

    private void handleRecovery(Socket socket) {
        try (Socket s = socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

            s.setSoTimeout(Wire.T_TCP_MS);
            String req = in.readLine();

            if (req == null) return;
            String[] p = req.trim().split("\\s+");

            if (p.length != 3 || !p[0].equals("PEDIDO")) {
                out.println("ERRO Pedido mal formado");
                return;
            }

            long from, to;
            try {
                from = Long.parseLong(p[1]);
                to = Long.parseLong(p[2]);
            } catch (NumberFormatException e) {
                out.println("ERRO Valores numéricos inválidos");
                return;
            }

            if (from > to) {
                out.println("ERRO Intervalo inválido (from > to)");
                return;
            }

            long top;
            synchronized (this) {
                top = Math.min(to, seq);
            }

            List<SensorRecord> records = file.range(from, top);

            System.out.println("RECUPERAÇÃO: " + from + ".." + top + " para " + s.getRemoteSocketAddress() + " (" + records.size() + " registos)");

            for (SensorRecord r : records) {
                out.println(r.toLine());
            }
            out.println("FIM");

        } catch (Exception e) {
            System.out.println("Erro na recuperação TCP p" + k + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Erro: Argumentos insuficientes.");
            System.out.println("Uso: java rep04.PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        int k, n;
        try {
            k = Integer.parseInt(args[0]);
            n = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Erro: Os parâmetros <k> e <N> têm de ser valores numéricos inteiros.");
            System.out.println("Uso: java rep04.PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        if (n <= 0 || k < 0 || k >= n) {
            System.out.println("Erro: Partição inválida (k=" + k + " para N=" + n + "). O valor de k deve estar compreendido entre 0 e " + (n - 1) + ".");
            System.out.println("Uso: java rep04.PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        try {
            new PartitionLeader(k, n, host, api);
        } catch (Exception e) {
            System.out.println("Erro crítico ao arrancar o PartitionLeader:");
            e.printStackTrace();
            System.exit(1);   // o agendador manteria o processo vivo sem subscrição
        }
    }
}