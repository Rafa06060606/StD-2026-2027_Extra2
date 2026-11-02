package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.Random;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1); // O datagrama não sai da rede local
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            Random rand = new Random();

            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break; // BLOQUEIA à espera do utilizador
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
                    System.out.println("A iniciar rajada de " + n + " registos...");
                    long tStart = System.currentTimeMillis();
                    for (int i = 0; i < n; i++) {
                        double temp = 15 + rand.nextDouble() * 15;
                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                        file.append(r);
                        byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                        socket.send(new DatagramPacket(m, m.length, group, PORT));
                    }
                    long tEnd = System.currentTimeMillis();
                    System.out.println("Rajada concluída. Primeiro envio: " + tStart + " ms | Último envio: " + tEnd + " ms (Total: " + (tEnd - tStart) + " ms)");
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
                file.append(r); // 1) Escreve localmente
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT)); // 2) Propaga
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}