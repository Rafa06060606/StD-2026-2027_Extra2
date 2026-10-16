package rep01;

import java.util.Scanner;
import java.util.Random;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubsub = new IpfsPubSub(api);
        
        try (Scanner sc = new Scanner(System.in)) {
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            Random rand = new Random();

            System.out.println("PubSub Líder pronto (API: " + api + "). Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                if (line.toLowerCase().startsWith("rajada")) {
                    String[] parts = line.split("\\s+");
                    int n = 1000;
                    if (parts.length == 2) {
                        try {
                            n = Integer.parseInt(parts[1]);
                        } catch (NumberFormatException e) {
                            System.out.println("Número inválido.");
                            continue;
                        }
                    }
                    System.out.println("A iniciar rajada IPFS de " + n + " registos...");
                    long tStart = System.currentTimeMillis();
                    for (int i = 0; i < n; i++) {
                        double temp = 15 + rand.nextDouble() * 15;
                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                        file.append(r);
                        pubsub.publish(TOPIC, r.toLine());
                    }
                    long tEnd = System.currentTimeMillis();
                    System.out.println("Rajada concluída. Primeiro: " + tStart + " ms | Último: " + tEnd + " ms (Total: " + (tEnd - tStart) + " ms)");
                    continue;
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);
                pubsub.publish(TOPIC, r.toLine());
                System.out.println("Registado e publicado: " + r.toLine());
            }
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}