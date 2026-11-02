/*
É a peça que cria dependências entre partições. Sem ele, os líderes não comunicam e todos os pares de partições 
são concorrentes. O processador observa as N partições, acumula o que viu e, ao ver temperatura ≥ 28, escreve 
noutra partição um ALARM-<sensor> que carrega esse conhecimento. Assim o alarme passa a depender causalmente do 
que o processador viu.

Ele não gera eventos próprios: não incrementa nada. É o líder de destino que faz o L+1 e o tick.
*/


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
    private long Lp = 0; // "L do processador": máximo de L que já viu
    private final VectorClock Vp; // "V do processador": máximo de V que já viu

    // Identifica os registos já processados:
    // mesma partição + mesma sequência = duplicado.
    private final Set<String> vistos = new HashSet<>();  // chaves "partição:seq" já processadas

    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.pubsub = new IpfsPubSub(api); // cliente da API do Kubo
        this.partitioner = new Partitioner(n); // para calcular a partição do ALARM-<sensor>
        this.Vp = new VectorClock(n); // começa a zeros: ainda não observou nada
    }

    public void start() {
        for (int k = 0; k < n; k++) {
            final int particao = k;  // a lambda só captura variáveis "efetivamente finais"

            Thread thread = new Thread(() -> {
                try {
                    String topic = Wire.topicData(particao);

                    System.out.println(
                            "Processador subscreve " + topic
                    );
 
                    pubsub.subscribe(topic, mensagem -> { // subscribe BLOQUEIA para sempre: é lá que a thread fica a ler linhas do daemon
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

            thread.setName("alarm-p" + k); // nome para depurar
            thread.start(); // arranca e o main continua
        }
    }

    private void processar(int k, String mensagem) { 
        try {
            // Formato:
            // REG;host;porto;seq;sensor;temperatura;instante;L;V
            String[] partes = mensagem.trim().split(";", 4); // REG;host;porto;<resto>  →  4 pedaços; o limite 4 é essencial: o "resto" contém ';' e fica inteiro

            if (partes.length != 4
                    || !partes[0].equals("REG")) {  // ignora o que não é REG
                return;
            }

            // O quarto campo contém o registo completo.
            SensorRecord r = SensorRecord.fromLine(partes[3]);  // lê seq;sensor;temp;instante;L;V

            String sensor = r.getSensorId();

            // Ignorar alarmes para não gerar alarmes de alarmes.
            if (sensor.startsWith("ALARM-")) { // sem "alarmes de alarmes"
                return;
            }

            long Lsnapshot;
            VectorClock Vsnapshot;

            synchronized (this) {

                // Ignorar duplicados da mesma partição.
                String chave = k + ":" + r.getSeq(); // Identifica o registo pela partição e sequência.

                if (!vistos.add(chave)) {  // Ignora registos que já foram processados.
                    return;
                }

                // Atualiza o maior relógio de Lamport conhecido.
                Lp = Math.max(Lp, r.getLamport()); // acumula Lamport
                Vp.merge(r.getVector()); // Combina o conhecimento causal do registo recebido.

                // Criar cópias consistentes para publicar.
                Lsnapshot = Lp; // Guarda uma fotografia independente dos relógios.
                Vsnapshot = Vp.copy(); // ...tirada no mesmo instante, dentro do lock

                // Falha proposital
                /*Vsnapshot = Vp;*/
            }

            // Só gera alarme quando a temperatura atinge o limiar.
            if (r.getTemperature() < Wire.LIMIAR_ALARME) { // abaixo de 28: não há alarme
                return;
            }

            String sensorAlarme = "ALARM-" + sensor; // Cria o identificador do sensor de alarme.

            int j = partitioner.of(sensorAlarme); // Descobre a partição que vai receber o alarme.

            String mensagemAlarme = Wire.in( // Constrói a mensagem com os relógios acumulados.
                    sensorAlarme,
                    r.getTemperature(),
                    Lsnapshot,
                    Vsnapshot
            );

            pubsub.publish( // Envia o alarme para o tópico de entrada da partição de destino.
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
