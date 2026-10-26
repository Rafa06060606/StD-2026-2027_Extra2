package rep03;

import java.util.Random;
import java.util.Scanner;

public class Producer {
    public static void main(String[] args) {
        int nPartitions = 1;
        String api = "http://127.0.0.1:5001/api/v0";

        if (args.length >= 1) {
            if (args[0].startsWith("http://") || args[0].startsWith("https://")) {
                api = args[0];
            } else {
                try {
                    nPartitions = Integer.parseInt(args[0]);
                    if (nPartitions <= 0) {
                        System.out.println("Erro: O número de partições <N> deve ser positivo.");
                        System.out.println("Uso: java rep02.Producer <N> [api]");
                        return;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Erro: <N> deve ser um número inteiro.");
                    System.out.println("Uso: java rep02.Producer <N> [api]");
                    return;
                }
                if (args.length >= 2) {
                    api = args[1];
                }
            }
        }

        Partitioner partitioner = new Partitioner(nPartitions);
        IpfsPubSub pubsub = new IpfsPubSub(api);
        Random rand = new Random();

        try (Scanner sc = new Scanner(System.in)) {
            System.out.println("Produtor pronto (N = " + nPartitions + "). Formato: <sensor> <temperatura> | rajada <n> <s> | rajada-quente <n> <s> | 'sair'");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;
                if (line.equalsIgnoreCase("sair")) break;

                String lower = line.toLowerCase();
                if (lower.startsWith("rajada-quente") || lower.startsWith("rajada")) {
                    boolean hot = lower.startsWith("rajada-quente");
                    String[] parts = line.split("\\s+");
                    int n = parts.length > 1 ? Integer.parseInt(parts[1]) : 5;
                    int sMax = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;

                    String cmdName = hot ? "rajada-quente" : "rajada";
                    int[] counts = new int[nPartitions];

                    long tStart = System.currentTimeMillis();
                    System.out.println(cmdName + " " + n + " " + sMax + ": primeira publicação em " + tStart);

                    for (int i = 0; i < n; i++) {
                        String sensor;
                        if (hot && rand.nextDouble() < 0.80) {
                            sensor = "S01";
                        } else {
                            int lo = (hot && sMax > 1) ? 2 : 1;
                            int sIndex = lo + rand.nextInt(sMax - lo + 1);
                            sensor = String.format("S%02d", sIndex);
                        }
                        double temp = Math.round((15.0 + rand.nextDouble() * 15.0) * 10.0) / 10.0;

                        int k = partitioner.of(sensor);
                        String msg = Wire.in(
                            sensor,
                            temp,
                            0,
                            new VectorClock(nPartitions)
                        );
                        pubsub.publish(Wire.topicIn(k), msg);
                        counts[k]++;
                    }
                    long tEnd = System.currentTimeMillis();
                    System.out.println(cmdName + " " + n + " " + sMax + ": última publicação em " + tEnd + " (Total: " + (tEnd - tStart) + " ms)");

                    StringBuilder sb = new StringBuilder(cmdName + ": ");
                    for (int k = 0; k < nPartitions; k++) {
                        if (k > 0) sb.append(" · ");
                        sb.append("p").append(k).append(" ").append(counts[k]);
                    }
                    System.out.println(sb);
                    continue;
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido. Usa: <sensor> <temperatura>");
                    continue;
                }

                String sensor = p[0];
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                int k = partitioner.of(sensor);
                String msg = Wire.in(
                    sensor,
                    temp,
                    0,
                    new VectorClock(nPartitions)
                );
                pubsub.publish(Wire.topicIn(k), msg);
                System.out.println("Publicado em " + Wire.topicIn(k) + " (p" + k + "): " + msg);
            }
        } catch (Exception e) {
            System.out.println("Erro no Produtor: " + e.getMessage());
        }
    }
}