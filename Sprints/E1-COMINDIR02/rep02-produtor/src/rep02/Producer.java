package rep02;

import java.util.Scanner;
import java.util.Random;

public class Producer {
    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubsub = new IpfsPubSub(api);
        Random rand = new Random();

        try (Scanner sc = new Scanner(System.in)) {
            System.out.println("Produtor pronto (Parte A). Formato: <sensor> <temperatura> | 'rajada <n> <s>' | 'sair'");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;
                if (line.equalsIgnoreCase("sair")) break;

                if (line.toLowerCase().startsWith("rajada")) {
                    String[] parts = line.split("\\s+");
                    int n = parts.length > 1 ? Integer.parseInt(parts[1]) : 5;
                    int sMax = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;

                    System.out.println("A iniciar rajada de " + n + " leituras (sensores S01 a S" + String.format("%02d", sMax) + ")...");
                    
                    long tStart = System.currentTimeMillis();
                    for (int i = 0; i < n; i++) {
                        int sIndex = 1 + rand.nextInt(sMax);
                        String sensor = String.format("S%02d", sIndex);
                        double temp = 15 + rand.nextDouble() * 15;
                        
                        String msg = Wire.in(sensor, temp);
                        pubsub.publish(Wire.topicIn(0), msg);
                    }
                    long tEnd = System.currentTimeMillis();

                    System.out.println("Rajada concluída.");
                    System.out.println("Primeira publicação: " + tStart + " ms | Última publicação: " + tEnd + " ms (Total: " + (tEnd - tStart) + " ms)");
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

                String msg = Wire.in(sensor, temp);
                pubsub.publish(Wire.topicIn(0), msg);
                System.out.println("Publicado em " + Wire.topicIn(0) + ": " + msg);
            }
        } catch (Exception e) {
            System.out.println("Erro no Produtor: " + e.getMessage());
        }
    }
}