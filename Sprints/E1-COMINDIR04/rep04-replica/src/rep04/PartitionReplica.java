package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PartitionReplica {
    // partilhados por todas as partições desta réplica (um monitor por partição)
    private static final List<HeartbeatMonitor> MONITORS = new CopyOnWriteArrayList<>();
    private static volatile MonitorLog LOG;

    private final String replicaId;
    private final int k;
    private final RecordFile file;
    private final HeartbeatMonitor monitor;
    private long seq;

    private String leaderHost = null;
    private int leaderPort = -1;
    private final TreeMap<Long, SensorRecord> buffered = new TreeMap<>();
    private boolean recovering = false;

    // Simulação de perdas (-Dperda=p e -Ddescartar=seq), aplicada a cada registo do lote
    private final double perdaProb;
    private Long descartarSeq;
    private final Random rand = new Random();

    public PartitionReplica(String replicaId, int k, String api) throws Exception {
        this.replicaId = replicaId;
        this.k = k;

        this.file = new RecordFile("replica-" + replicaId + "-p" + k + ".txt");
        this.seq = file.lastSeq();

        this.monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, LOG::event);
        MONITORS.add(monitor);

        double p = 0.0;
        String propPerda = System.getProperty("perda");
        if (propPerda != null) {
            try { p = Double.parseDouble(propPerda); } catch (NumberFormatException ignored) {}
        }
        this.perdaProb = p;

        Long dSeq = null;
        String propDesc = System.getProperty("descartar");
        if (propDesc != null) {
            try { dSeq = Long.parseLong(propDesc); } catch (NumberFormatException ignored) {}
        }
        this.descartarSeq = dSeq;

        IpfsPubSub pubsub = new IpfsPubSub(api);

        System.out.println("Réplica " + replicaId + " subscreve " + Wire.topicData(k) + " (lastSeq() = " + seq + ")");

        pubsub.subscribe(Wire.topicData(k), msg -> {
            try {
                Batch b;
                try {
                    b = Batch.decode(msg, -1);   // dimensão do vetor deduzida de cada linha
                } catch (RuntimeException e) {
                    System.out.println("REJEITADO: " + (msg == null ? "" : msg.trim()) + " (" + e.getMessage() + ")");
                    return;
                }

                synchronized (this) {
                    // aprende o endereço e porto anunciados pelo líder
                    leaderHost = b.address;
                    leaderPort = b.port;

                    // o monitor ignora lotes de encarnações antigas, mas os registos continuam a ser aplicados
                    monitor.onHeartbeat(b.inc, b.hb);
                    String intervalo = b.records.isEmpty() ? "-"
                            : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq();
                    LOG.event("RECEBIDO;p" + k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                            + ";n=" + b.n + ";seq=" + intervalo);

                    for (SensorRecord r : b.records) {
                        long rSeq = r.getSeq();
                        if (descartarSeq != null && rSeq == descartarSeq) {
                            descartarSeq = null; // só a primeira receção
                            System.out.println("DESCARTADO (simulação): " + rSeq);
                            continue;
                        }
                        if (perdaProb > 0.0 && rand.nextDouble() < perdaProb) {
                            System.out.println("DESCARTADO (simulação): " + rSeq);
                            continue;
                        }
                        aplicar(r, false);
                        // regra da COMINDIR02: demasiados retidos → recupera o intervalo em falta
                        if (buffered.size() > Wire.LIMITE_RETIDOS) {
                            recoverRange(seq + 1, buffered.firstKey() - 1);
                        }
                    }

                    // regra nova: se o último seq aplicado for inferior a ultimoSeq, faltam registos
                    if (seq < b.lastSeq) {
                        recoverRange(seq + 1, b.lastSeq);
                    }
                }
            } catch (Exception e) {
                System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
            }
        });
    }

    private synchronized void aplicar(SensorRecord r, boolean fromTcp) throws IOException {
        long rSeq = r.getSeq();

        if (rSeq <= seq || buffered.containsKey(rSeq)) {
            if (!fromTcp) {
                System.out.println("DUPLICADO: " + rSeq);
            }
            return;
        }

        if (rSeq == seq + 1) {
            file.append(r);
            seq = rSeq;
            if (fromTcp) {
                System.out.println("RECUPERADO: " + seq);
            } else {
                System.out.println("Aplicado: " + r.toLine());
            }
            escreverEmSequencia();
        } else {
            buffered.put(rSeq, r);
            System.out.println("EM ESPERA: " + rSeq + " (falta o registo " + (seq + 1) + ") · retidos " + buffered.size());
        }
    }

    private synchronized void escreverEmSequencia() throws IOException {
        while (!buffered.isEmpty() && buffered.firstKey() == seq + 1) {
            SensorRecord nextR = buffered.pollFirstEntry().getValue();
            file.append(nextR);
            seq = nextR.getSeq();
            System.out.println("Aplicado (estava retido): " + nextR.toLine());
        }
    }

    // pede ao líder, por TCP, os registos com from <= seq <= to
    private void recoverRange(long from, long to) {
        String host;
        int port;

        synchronized (this) {
            if (recovering) return;
            if (leaderPort == -1 || leaderHost == null) return;
            if (from > to) return;

            host = leaderHost;
            port = leaderPort;
            recovering = true;
        }

        System.out.println("RECUPERAÇÃO: " + from + ".." + to + " pedida a " + host + ":" + port);
        LOG.event("RECUPERACAO;p" + k + ";de=" + from + ";ate=" + to);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            socket.setSoTimeout(Wire.T_TCP_MS);

            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

                out.println("PEDIDO " + from + " " + to);

                String line;
                boolean receivedFim = false;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    if (line.equals("FIM")) {
                        receivedFim = true;
                        break;
                    }
                    if (line.startsWith("ERRO")) {
                        System.out.println("RECUPERAÇÃO FALHOU: " + line);
                        break;
                    }

                    SensorRecord r = SensorRecord.fromLine(line);
                    synchronized (this) {
                        aplicar(r, true);
                    }
                }

                if (!receivedFim) {
                    System.out.println("RECUPERAÇÃO FALHOU: ligação terminou sem FIM. Estado retido mantido em memória.");
                } else {
                    synchronized (this) {
                        escreverEmSequencia();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
        } finally {
            synchronized (this) {
                recovering = false;
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Erro: Argumentos insuficientes.");
            System.out.println("Uso: java rep04.PartitionReplica <id> <partições> [api|portoApi]");
            return;
        }

        String replicaId = args[0];
        String partitionsArg = args[1];
        String api = "http://127.0.0.1:5001/api/v0";
        if (args.length > 2) {
            if (args[2].startsWith("http://") || args[2].startsWith("https://")) {
                api = args[2];
            } else {
                api = "http://127.0.0.1:" + args[2] + "/api/v0";
            }
        }

        LOG = new MonitorLog("replica" + replicaId);

        // thread de verificação: chama check() em todos os monitores a cada VERIFICACAO_MS
        Thread checker = new Thread(() -> {
            while (true) {
                for (HeartbeatMonitor m : MONITORS) {
                    m.check();
                }
                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        checker.setDaemon(true);
        checker.start();

        String finalApi = api;
        String[] parts = partitionsArg.split(",");
        for (String pStr : parts) {
            try {
                int k = Integer.parseInt(pStr.trim());
                new Thread(() -> {
                    try {
                        new PartitionReplica(replicaId, k, finalApi);
                    } catch (Exception e) {
                        System.out.println("Erro crítico na réplica p" + k + ": " + e.getMessage());
                    }
                }).start();
            } catch (NumberFormatException e) {
                System.out.println("Erro: Identificador de partição inválido: " + pStr);
            }
        }
    }
}