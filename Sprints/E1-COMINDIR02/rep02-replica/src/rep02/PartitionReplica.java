package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;

public class PartitionReplica {
    private final String replicaId;
    private final int k;
    private final RecordFile file;
    private long seq;

    private String leaderHost = null;
    private int leaderPort = -1;
    private final TreeMap<Long, SensorRecord> buffered = new TreeMap<>();
    private boolean recovering = false;

    // Simulação de perdas (-Dperda=p e -Ddescartar=seq)
    private final double perdaProb;
    private Long descartarSeq;
    private final Random rand = new Random();

    public PartitionReplica(String replicaId, int k, String api) throws Exception {
        this.replicaId = replicaId;
        this.k = k;

        this.file = new RecordFile("replica-" + replicaId + "-p" + k + ".txt");
        this.seq = file.lastSeq();

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
                String trimmed = msg != null ? msg.trim() : "";
                String[] parts;
                try {
                    parts = Wire.regParts(trimmed);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + trimmed);
                    return;
                }

                String host = parts[0];
                int port;
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("REJEITADO: " + trimmed);
                    return;
                }

                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(parts[2]);
                } catch (Exception e) {
                    System.out.println("REJEITADO: " + trimmed);
                    return;
                }

                long rSeq = r.getSeq();

                // Simulação de descarte explícito (-Ddescartar=seq) ou probabilístico (-Dperda=p)
                synchronized (this) {
                    if (descartarSeq != null && rSeq == descartarSeq) {
                        descartarSeq = null; // Descarta apenas na primeira vez que chega pelo PubSub
                        System.out.println("DESCARTADO (simulação): " + rSeq);
                        return;
                    }
                    if (perdaProb > 0.0 && rand.nextDouble() < perdaProb) {
                        System.out.println("DESCARTADO (simulação): " + rSeq);
                        return;
                    }

                    // Aprende o endereço e porto anunciados pelo líder
                    leaderHost = host;
                    leaderPort = port;

                    aplicar(r, false);

                    // Se o número de retidos exceder LIMITE_RETIDOS (10), recupera na própria thread de subscrição
                    if (buffered.size() > Wire.LIMITE_RETIDOS) {
                        recoverFromLeader();
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

    private void recoverFromLeader() {
        long from;
        long to;
        String host;
        int port;

        synchronized (this) {
            if (recovering || buffered.isEmpty()) return;
            if (leaderPort == -1 || leaderHost == null) return;

            from = seq + 1;
            to = buffered.firstKey() - 1;
            if (from > to) return;

            host = leaderHost;
            port = leaderPort;
            recovering = true;
        }

        System.out.println("RECUPERAÇÃO: " + from + ".." + to + " pedida a " + host + ":" + port);

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
            System.out.println("Uso: java rep02.PartitionReplica <id> <partições> [api|portoApi]");
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