
package rep03;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {

    private static final String API_POR_OMISSAO =
            "http://127.0.0.1:5001/api/v0";

    private final int n;
    private final IpfsPubSub pubsub;
    private final Partitioner partitioner;

    // Relógios partilhados pelas threads das partições.
    private long Lp = 0;
    private final VectorClock Vp;

    // Identifica os registos já processados:
    // mesma partição + mesma sequência = duplicado.
    private final Set<String> vistos = new HashSet<>();

    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.pubsub = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);
        this.Vp = new VectorClock(n);
    }

    public void start() {
        for (int k = 0; k < n; k++) {
            final int particao = k;

            Thread thread = new Thread(() -> {
                try {
                    String topic = Wire.topicData(particao);

                    System.out.println(
                            "Processador subscreve " + topic
                    );

                    pubsub.subscribe(topic, mensagem -> {
                        processar(particao, mensagem);
                    });

                } catch (IOException | InterruptedException e) {
                    System.err.println(
                            "Erro na subscrição da partição "
                                    + particao + ": "
                                    + e.getMessage()
                    );

                    Thread.currentThread().interrupt();
                }
            });

            thread.setName("alarm-p" + k);
            thread.start();
        }
    }

    private void processar(int k, String mensagem) {
        try {
            // Formato:
            // REG;host;porto;seq;sensor;temperatura;instante;L;V
            String[] partes = mensagem.trim().split(";", 4);

            if (partes.length != 4
                    || !partes[0].equals("REG")) {
                return;
            }

            // O quarto campo contém o registo completo.
            SensorRecord r = SensorRecord.fromLine(partes[3]);

            String sensor = r.getSensorId();

            // Ignorar alarmes para não gerar alarmes de alarmes.
            if (sensor.startsWith("ALARM-")) {
                return;
            }

            long Lsnapshot;
            VectorClock Vsnapshot;

            synchronized (this) {

                // Ignorar duplicados da mesma partição.
                String chave = k + ":" + r.getSeq();

                if (!vistos.add(chave)) {
                    return;
                }

                // Acumular o conhecimento de todos os registos.
                Lp = Math.max(Lp, r.getLamport());
                Vp.merge(r.getVector());

                // Criar cópias consistentes para publicar.
                Lsnapshot = Lp;
                Vsnapshot = Vp.copy();

                // Falha proposital
                /*Vsnapshot = Vp;*/
            }

            // Temperaturas abaixo do limiar não geram alarmes.
            if (r.getTemperature() < Wire.LIMIAR_ALARME) {
                return;
            }

            String sensorAlarme = "ALARM-" + sensor;

            int j = partitioner.of(sensorAlarme);

            String mensagemAlarme = Wire.in(
                    sensorAlarme,
                    r.getTemperature(),
                    Lsnapshot,
                    Vsnapshot
            );

            pubsub.publish(
                    Wire.topicIn(j),
                    mensagemAlarme
            );

            System.out.println(
                    "ALARME: " + sensor
                            + " p" + k + "#" + r.getSeq()
                            + " -> p" + j
            );

        } catch (Exception e) {
            System.err.println(
                    "Erro ao processar registo: "
                            + e.getMessage()
            );
        }
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.out.println(
                    "Uso: AlarmProcessor <N> [api]"
            );
            return;
        }

        int n = Integer.parseInt(args[0]);

        String api = args.length == 2
                ? args[1]
                : API_POR_OMISSAO;

        if (n < 1) {
            throw new IllegalArgumentException(
                    "N tem de ser maior que zero."
            );
        }

        new AlarmProcessor(n, api).start();
    }
}
