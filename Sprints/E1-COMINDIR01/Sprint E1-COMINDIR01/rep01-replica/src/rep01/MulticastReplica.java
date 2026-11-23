package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group); // Junta-se ao grupo multicast
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);

            long[] expectedSeq = {file.lastSeq() + 1};
            TreeMap<Long, SensorRecord> pending = new TreeMap<>();
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p); 
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8).trim();
                
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (Exception e) {
                    System.out.println("REJEITADO: " + line);
                    continue;
                }

                long seq = r.getSeq();

                if (seq < expectedSeq[0]) {
                    System.out.println("DUPLICADO: " + seq);
                } else if (seq == expectedSeq[0]) {
                    file.append(r);
                    System.out.println("Aplicado: " + r.toLine());
                    expectedSeq[0]++;

                    // Descarrega do buffer os registos seguintes que já tenham chegado
                    while (pending.containsKey(expectedSeq[0])) {
                        SensorRecord nextR = pending.remove(expectedSeq[0]);
                        file.append(nextR);
                        System.out.println("Aplicado (do buffer): " + nextR.toLine());
                        expectedSeq[0]++;
                    }
                } else {
                    pending.put(seq, r);
                    System.out.println("EM ESPERA: " + seq + " (falta o registo " + expectedSeq[0] + ")");
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}