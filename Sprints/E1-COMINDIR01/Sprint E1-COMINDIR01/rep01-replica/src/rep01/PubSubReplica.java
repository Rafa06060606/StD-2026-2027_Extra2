package rep01;

import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String id = args.length > 0 && !args[0].startsWith("http") ? args[0] : "1";
        String api = "http://127.0.0.1:5001/api/v0";
        if (args.length > 0 && args[0].startsWith("http")) {
            api = args[0];
        } else if (args.length > 1) {
            api = args[1];
        }

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        IpfsPubSub pubsub = new IpfsPubSub(api);

        try {
            System.out.println("Réplica PubSub " + id + " à escuta no tópico " + TOPIC + " (API: " + api + ")");
            long[] expectedSeq = {file.lastSeq() + 1};
            TreeMap<Long, SensorRecord> pending = new TreeMap<>();

            pubsub.subscribe(TOPIC, message -> {
                String line = message.trim();
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (Exception e) {
                    System.out.println("REJEITADO: " + line);
                    return;
                }

                long seq = r.getSeq();
                try {
                    if (seq < expectedSeq[0]) {
                        System.out.println("DUPLICADO: " + seq);
                    } else if (seq == expectedSeq[0]) {
                        file.append(r);
                        System.out.println("Aplicado: " + r.toLine());
                        expectedSeq[0]++;

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
                } catch (Exception ex) {
                    System.out.println("Erro ao gravar no ficheiro: " + ex.getMessage());
                }
            });

        } catch (Exception e) {
            System.out.println("Erro na subscrição PubSub: " + e.getMessage());
        }
    }
}