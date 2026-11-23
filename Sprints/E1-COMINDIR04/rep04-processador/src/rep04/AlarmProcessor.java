/*
É a peça que cria dependências entre partições. Sem ele, os líderes não comunicam e todos os pares de partições 
são concorrentes. O processador observa as N partições, acumula o que viu e, ao ver temperatura ≥ 28, escreve 
noutra partição um ALARM-<sensor> que carrega esse conhecimento. Assim o alarme passa a depender causalmente do 
que o processador viu.

Ele não gera eventos próprios: não incrementa nada. É o líder de destino que faz o L+1 e o tick.
*/

package rep04;

import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {
    private final int n;
    private final String api;
    private final Partitioner partitioner;
    private final IpfsPubSub publisher;
    private final HeartbeatMonitor[] monitors;
    private final MonitorLog log = new MonitorLog("processador");

    // estado partilhado pelas threads de subscrição, protegido por 'lock'
    private final Object lock = new Object();
    private long lp = 0;
    private final VectorClock vp;
    private final Set<String> vistos = new HashSet<>();   // "k#seq"

    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.api = api;
        this.partitioner = new Partitioner(n);
        this.publisher = new IpfsPubSub(api);
        this.vp = new VectorClock(n);
        this.monitors = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::event);
        }
    }

    public void start() {
        for (int k = 0; k < n; k++) {
            final int part = k;
            new Thread(() -> listen(part)).start();
        }
        Thread checker = new Thread(() -> {
            while (true) {
                for (HeartbeatMonitor m : monitors) {
                    m.check();
                }
                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        checker.setDaemon(true);
        checker.start();
        System.out.println("Processador de alarmes (N=" + n + ", LIMIAR_ALARME=" + Wire.LIMIAR_ALARME
                + "): subscreve " + n + " partições");
    }

    private void listen(int k) {
        try {
            new IpfsPubSub(api).subscribe(Wire.topicData(k), msg -> onBatch(k, msg));
        } catch (Exception e) {
            System.out.println("Erro na subscrição de p" + k + ": " + e.getMessage());
        }
    }

    private void onBatch(int k, String msg) {
        Batch b;
        try {
            b = Batch.decode(msg, n);   // rejeita vetores de dimensão diferente de N
        } catch (RuntimeException e) {
            System.out.println("REJEITADO: " + (msg == null ? "" : msg.trim()) + " (" + e.getMessage() + ")");
            return;
        }

        monitors[k].onHeartbeat(b.inc, b.hb);
        String intervalo = b.records.isEmpty() ? "-"
                : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq();
        log.event("RECEBIDO;p" + k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                + ";n=" + b.n + ";seq=" + intervalo);

        for (SensorRecord r : b.records) {
            onRecord(k, r);
        }
    }

    // regras da COMINDIR03 aplicadas a cada registo do lote
    private void onRecord(int k, SensorRecord r) {
        if (r.getSensorId().startsWith("ALARM-")) {
            return;   // alarmes não geram alarmes
        }

        long lSnap;
        VectorClock vSnap;
        synchronized (lock) {
            if (!vistos.add(k + "#" + r.getSeq())) {
                return;   // duplicado (mesma partição, mesmo seq)
            }
            lp = Math.max(lp, r.getLamport());
            vp.merge(r.getVector());
            lSnap = lp;
            vSnap = vp.copy();   // cópia: o vetor partilhado continua a ser alterado por outras threads
        }

        if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
            String alarmSensor = "ALARM-" + r.getSensorId();
            int j = partitioner.of(alarmSensor);
            if (monitors[j].state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println("AVISO: líder p" + j + " suspeito; alarme " + alarmSensor + " pode perder-se");
            }
            try {
                publisher.publish(Wire.topicIn(j),
                        Wire.in(alarmSensor, r.getTemperature(), lSnap, vSnap));
                System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + j);
            } catch (Exception e) {
                System.out.println("Falha ao publicar alarme: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java rep04.AlarmProcessor <N> [api]");
            return;
        }
        int n;
        try {
            n = Integer.parseInt(args[0]);
            if (n <= 0) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            System.out.println("Erro: <N> deve ser um inteiro positivo.");
            return;
        }
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        new AlarmProcessor(n, api).start();
    }
}