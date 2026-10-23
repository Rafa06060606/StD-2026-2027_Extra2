package rep03;


import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.time.LocalDateTime;



public class PartitionLeader {
    private final int k;
    private final int n;
    private final int port;
    private final Partitioner partitioner;
    private final RecordFile file;
    private long seq;
    private long lamport;
    private VectorClock vector;



    public PartitionLeader(int k, int n, String host, String api) throws Exception {
        this.k = k;
        this.n = n;
        this.port = Wire.PORTO_BASE + k;
        this.partitioner = new Partitioner(n);      

        this.file = new RecordFile("lider-p" + k + ".txt");

        SensorRecord ultimo = file.last();

        if (ultimo == null) {
            this.seq = 0;
            this.lamport = 0;
            this.vector = new VectorClock(n);
        } else {
            this.seq = ultimo.getSeq();
            this.lamport = ultimo.getLamport();
            this.vector = ultimo.getVector();
        }

        // FALHA DELIBERADA:
        // o líder ignora o estado persistido após reiniciar.
        /*this.seq = 0;
        this.lamport = 0;
        this.vector = new VectorClock(n);*/

        IpfsPubSub pubsub = new IpfsPubSub(api);

        // Lança o servidor TCP de recuperação numa thread separada
        new Thread(this::startTcpServer).start();

        System.out.println("Líder p" + k + " (N=" + n + "): lastSeq() = " + seq + ", subscreve " + Wire.topicIn(k) + ", à escuta em " + host + ":" + port);

        // Subscreve o tópico de entrada da partição k no PubSub
        pubsub.subscribe(Wire.topicIn(k), msg -> {
            try {
                String trimmedMsg = msg != null ? msg.trim() : "";
                String[] parts = trimmedMsg.split(";");

                // A mensagem IN tem de conter 5 campos.
                if (parts.length != 5
                        || !parts[0].equals("IN")
                        || parts[1].isEmpty()) {
                    System.out.println("REJEITADO: " + trimmedMsg);
                    return;
                }

                String sensor = parts[1];
                double temp;
                long lamportRecebido;
                VectorClock vectorRecebido;

                try {
                    temp = Double.parseDouble(parts[2]);
                    lamportRecebido = Long.parseLong(parts[3]);
                    vectorRecebido = VectorClock.parse(parts[4], n);
                } catch (RuntimeException e) {
                    System.out.println("REJEITADO: " + trimmedMsg);
                    return;
                }

                // Confirma que o sensor pertence a esta partição.
                int expectedK = partitioner.of(sensor);

                if (expectedK != k) {
                    System.out.println(
                        "PARTIÇÃO ERRADA: " + trimmedMsg
                        + " (pertence a p" + expectedK + ")"
                    );
                    return;
                }

                SensorRecord r;
                synchronized (this) {
                    // Atualiza o relógio de Lamport.
                    lamport = Math.max(lamport, lamportRecebido) + 1;

                    // FALHA DELIBERADA: ignora o Lamport recebido.
                    /*lamport = lamport + 1;*/

                    // Atualiza o relógio vetorial.
                    vector.merge(vectorRecebido);
                    vector.tick(k);

                    // Falha proposital para testar a recuperação de registos.
                    /*vector.tick(k);*/

                    // Atribui a sequência local.
                    seq++;
                    // O componente local do vetor deve coincidir com a sequência.
                    if (vector.get(k) != seq) {
                        throw new IllegalStateException(
                            "Relógio vetorial inconsistente em p" + k
                            + ": V[" + k + "]=" + vector.get(k)
                            + ", seq=" + seq
                        );
                    }

                    r = new SensorRecord(
                        seq,
                        sensor,
                        temp,
                        LocalDateTime.now().withNano(0).toString(),
                        lamport,
                        vector.copy()
                    );
                    file.append(r);
                }

                // Publica o registo já atualizado.
                String reg = Wire.reg(host, port, r);
                pubsub.publish(Wire.topicData(k), reg);

                System.out.println(
                    "EVENTO: seq=" + r.getSeq()
                    + " L=" + r.getLamport()
                    + " V=" + r.getVector()
                );
            } catch (Exception e) {
                System.out.println(
                    "REJEITADO: " + msg + " (Erro: " + e.getMessage() + ")"
                );
            }
        });
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
            System.out.println("Uso: java rep02.PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        int k, n;
        try {
            k = Integer.parseInt(args[0]);
            n = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Erro: Os parâmetros <k> e <N> têm de ser valores numéricos inteiros.");
            System.out.println("Uso: java rep03.PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        if (n <= 0 || k < 0 || k >= n) {
            System.out.println("Erro: Partição inválida (k=" + k + " para N=" + n + "). O valor de k deve estar compreendido entre 0 e " + (n - 1) + ".");
            System.out.println("Uso: java rep03.PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        try {
          new PartitionLeader(k, n, host, api);
        } catch (Exception e) {
            System.out.println("Erro crítico ao arrancar o PartitionLeader:");
            e.printStackTrace();
        }
    }
}