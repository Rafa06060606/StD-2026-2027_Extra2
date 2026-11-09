package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.Scanner;

// Produtor: lê comandos do teclado e publica leituras (IN) no tópico da partição certa.
// Não sabe quem é o líder: publica para a partição e o líder é que aceita.
// Em paralelo "escuta" os lotes de todas as partições só para saber se há líder vivo.
public class Producer {

    private static HeartbeatMonitor[] monitors;
    private static final Path FICHEIRO = Paths.get("produtor.txt");
    private static final Object ficheiroLock = new Object();

    // Regista a leitura em produtor.txt: instante;p<k>;sensor;temperatura
    // (serve ao RaftCheck para contar leituras perdidas)
    private static void registar(int k, String sensor, double temp) {
        String linha = System.currentTimeMillis() + ";p" + k + ";" + sensor + ";" + temp
                + System.lineSeparator();
        synchronized (ficheiroLock) {
            try {
                Files.writeString(FICHEIRO, linha, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("Erro a escrever " + FICHEIRO + ": " + e);
            }
        }
    }

    // Avisa (sem impedir o envio) quando não há um líder vivo conhecido
    private static void avisar(int k, String sensor) {
        HeartbeatMonitor.State s = monitors[k].state();
        if (s != HeartbeatMonitor.State.ATIVO) {
            System.out.println("AVISO: partição p" + k + " sem líder conhecido; leitura "
                    + sensor + " pode perder-se");
        }
    }

    // Envia uma leitura: aviso, publicação e registo em produtor.txt
    private static void enviar(IpfsPubSub pubsub, Partitioner partitioner, int n,
                               String sensor, double temp) throws IOException, InterruptedException {
        int k = partitioner.of(sensor);
        avisar(k, sensor);
        pubsub.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(n)));
        registar(k, sensor, temp);
    }

    // Uma thread por partição: subscribe() bloqueia, e se o IPFS cair volta a ligar-se
    private static void escutar(int k, String api, MonitorLog log) {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    new IpfsPubSub(api).subscribe(Wire.topicData(k), msg -> {
                        try {
                            Batch h = Batch.decodeHeader(msg);
                            monitors[k].onHeartbeat(h.term, h.leader, h.hb);
                        } catch (RuntimeException e) {
                            System.out.println("REJEITADO: LOTE ilegível (" + e + ")");
                        }
                    });
                } catch (Exception e) {
                    log.event("ERRO;p" + k + ";subscricao=" + Wire.topicData(k) + ";" + e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) {
        int n = 1;
        String api = "http://127.0.0.1:5001/api/v0";

        if (args.length >= 1) {
            if (args[0].startsWith("http://") || args[0].startsWith("https://")) {
                api = args[0];
            } else {
                try {
                    n = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    n = -1;
                }
                if (n <= 0) {
                    System.out.println("Erro: <N> deve ser um inteiro positivo.");
                    System.out.println("Uso: java raft04.Producer <N> [api]");
                    return;
                }
                if (args.length >= 2) api = args[1];
            }
        }

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub pubsub = new IpfsPubSub(api);
        Random rand = new Random();

        // um monitor por partição, alimentado pelos cabeçalhos dos lotes
        MonitorLog log = new MonitorLog("produtor");
        monitors = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::event);
            escutar(k, api, log);
        }
        Thread checker = new Thread(() -> {
            while (true) {
                for (HeartbeatMonitor m : monitors) m.check();
                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        checker.setDaemon(true);
        checker.start();

        try (Scanner sc = new Scanner(System.in)) {
            System.out.println("Produtor pronto (N = " + n + "). Formato: <sensor> <temperatura> | "
                    + "rajada <n> <s> | rajada-quente <n> <s> | 'sair'");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;
                if (line.equalsIgnoreCase("sair")) break;

                try {
                    String lower = line.toLowerCase();
                    if (lower.startsWith("rajada")) {
                        boolean hot = lower.startsWith("rajada-quente");
                        String[] parts = line.split("\\s+");
                        int total = parts.length > 1 ? Integer.parseInt(parts[1]) : 5;
                        int sMax = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;
                        int[] counts = new int[n];

                        long t0 = System.currentTimeMillis();
                        for (int j = 0; j < total; j++) {
                            String sensor;
                            if (hot && rand.nextDouble() < 0.80) {
                                sensor = "S01";
                            } else {
                                int lo = (hot && sMax > 1) ? 2 : 1;
                                sensor = String.format("S%02d", lo + rand.nextInt(sMax - lo + 1));
                            }
                            double temp = Math.round((15.0 + rand.nextDouble() * 15.0) * 10.0) / 10.0;
                            enviar(pubsub, partitioner, n, sensor, temp);
                            counts[partitioner.of(sensor)]++;
                        }
                        long t1 = System.currentTimeMillis();
                        StringBuilder sb = new StringBuilder(parts[0] + " " + total + ": ");
                        for (int k = 0; k < n; k++) {
                            if (k > 0) sb.append(" · ");
                            sb.append("p").append(k).append(" ").append(counts[k]);
                        }
                        System.out.println(sb + " (" + (t1 - t0) + " ms)");
                        continue;
                    }

                    String[] p = line.split("\\s+");
                    if (p.length != 2) {
                        System.out.println("Formato inválido. Usa: <sensor> <temperatura>");
                        continue;
                    }
                    double temp = Double.parseDouble(p[1]);
                    if (Double.isNaN(temp) || Double.isInfinite(temp)) {
                        System.out.println("Temperatura inválida: " + p[1]);
                        continue;
                    }
                    enviar(pubsub, partitioner, n, p[0], temp);
                    System.out.println("Publicado em " + Wire.topicIn(partitioner.of(p[0]))
                            + " (p" + partitioner.of(p[0]) + "): " + p[0] + " " + temp);
                } catch (NumberFormatException e) {
                    System.out.println("Número inválido: " + e.getMessage());
                } catch (IOException e) {
                    System.out.println("Erro ao publicar (IPFS?): " + e);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}