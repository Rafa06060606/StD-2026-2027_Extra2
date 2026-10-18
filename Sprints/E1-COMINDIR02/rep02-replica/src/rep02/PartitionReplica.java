package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
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

    public PartitionReplica(String replicaId, int k, String api) throws Exception {
        this.replicaId = replicaId;
        this.k = k;
        
        this.file = new RecordFile("replica-" + replicaId + "-p" + k + ".txt");
        this.seq = file.lastSeq();

        IpfsPubSub pubsub = new IpfsPubSub(api);

        System.out.println("Réplica " + replicaId + " (p" + k + ") iniciada. Último seq local: " + seq);

        new Thread(this::initialRecovery).start();

        pubsub.subscribe(Wire.topicData(k), msg -> {
            try {
                String trimmed = msg != null ? msg.trim() : "";
                if (!trimmed.startsWith("REG")) {
                    System.out.println("REJEITADO; Mensagem inválida: " + trimmed);
                    return;
                }

                String[] parts = trimmed.split(";", 4);
                if (parts.length < 4) {
                    System.out.println("REJEITADO; Formato REG incompleto: " + trimmed);
                    return;
                }

                String host = parts[1];
                int port;
                try {
                    port = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    System.out.println("REJEITADO; Porto inválido no REG: " + trimmed);
                    return;
                }

                synchronized (this) {
                    leaderHost = host;
                    leaderPort = port;
                }

                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(parts[3]);
                } catch (Exception e) {
                    System.out.println("REJEITADO; Registo de sensor corrompido: " + trimmed);
                    return;
                }

                long rSeq = r.getSeq();

                synchronized (this) {
                    if (rSeq <= seq) {
                        System.out.println("DUPLICADO; Registo " + rSeq + " já processado.");
                        return;
                    }

                    if (rSeq == seq + 1) {
                        file.append(r);
                        seq = r.getSeq();
                        System.out.println("p" + k + " Sincronizado (REG): " + r.toLine());

                        while (buffered.containsKey(seq + 1)) {
                            SensorRecord nextR = buffered.remove(seq + 1);
                            file.append(nextR);
                            seq = nextR.getSeq();
                            System.out.println("p" + k + " Sincronizado (Buffer): " + nextR.toLine());
                        }
                    } else {
                        if (!buffered.containsKey(rSeq)) {
                            buffered.put(rSeq, r);
                            System.out.println("EM ESPERA; Retido seq " + rSeq + ". Total retidos: " + buffered.size());
                        }

                        // Chamada direta e síncrona na thread de subscrição quando ultrapassa 10 retidos[cite: 10]
                        if (buffered.size() > 10) {
                            recoverFromLeader();
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("REJEITADO; Erro ao processar dados: " + e.getMessage());
            }
        });
    }

    private void initialRecovery() {
        int tentativas = 0;
        while (leaderPort == -1 && tentativas < 5) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
            tentativas++;
        }
        if (leaderPort != -1) {
            recoverFromLeader();
        }
    }

    private void recoverFromLeader() {
        synchronized (this) {
            if (recovering) return;
            if (leaderPort == -1 || leaderHost == null) return;
            recovering = true;
        }

        long from;
        long to;
        String host;
        int port;

        synchronized (this) {
            from = seq + 1;
            if (buffered.isEmpty()) {
                to = seq + 5000; 
            } else {
                to = buffered.firstKey() - 1;
            }

            if (from > to) {
                synchronized (this) {
                    recovering = false;
                }
                return;
            }

            host = leaderHost;
            port = leaderPort;
        }

        System.out.println("RECUPERAÇÃO: " + from + ".." + to + " pedida a " + host + ":" + port);

        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            socket.setSoTimeout(Wire.T_TCP_MS);
            out.println("PEDIDO " + from + " " + to);

            String line;
            boolean receivedFim = false;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.equals("FIM")) {
                    receivedFim = true;
                    break;
                }
                if (line.startsWith("ERRO")) {
                    System.out.println("Líder rejeitou recuperação: " + line);
                    break;
                }

                SensorRecord r = SensorRecord.fromLine(line);
                synchronized (this) {
                    if (r.getSeq() > seq) {
                        file.append(r);
                        seq = r.getSeq();
                        buffered.remove(seq);
                        // Mensagem exata exigida pela ficha[cite: 10]
                        System.out.println("RECUPERADO: " + seq);
                        System.out.println("p" + k + " Recuperado TCP: " + r.toLine());
                    }
                }
            }

            if (!receivedFim) {
                System.out.println("Aviso: Recuperação TCP incompleta (sem FIM). Estado retido mantido em memória.");
            } else {
                synchronized (this) {
                    while (buffered.containsKey(seq + 1)) {
                        SensorRecord nextR = buffered.remove(seq + 1);
                        file.append(nextR);
                        seq = nextR.getSeq();
                        System.out.println("p" + k + " Sincronizado pós-TCP (Buffer): " + nextR.toLine());
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("Aviso: Falha na ligação TCP para recuperação (" + e.getMessage() + "). Estado retido mantido.");
        } finally {
            synchronized (this) {
                recovering = false;
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Erro: Argumentos insuficientes.");
            System.out.println("Uso: java rep02.PartitionReplica <id> <partição(ões)> [api]");
            return;
        }

        String replicaId = args[0];
        String partitionsArg = args[1];
        String api = args.length > 2 ? args[2] : "http://127.0.0.1:5001/api/v0";

        String[] parts = partitionsArg.split(",");
        for (String pStr : parts) {
            try {
                int k = Integer.parseInt(pStr.trim());
                new Thread(() -> {
                    try {
                        new PartitionReplica(replicaId, k, api);
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