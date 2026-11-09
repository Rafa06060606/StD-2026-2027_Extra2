/*
É a peça que cria dependências entre partições: observa as N partições, acumula o que viu (L e V)
e, ao ver temperatura >= 28, escreve noutra partição um ALARM-<sensor> que leva esse conhecimento.
Não gera eventos próprios: é o líder de destino que faz o L+1 e o tick.

Com eleição de líder há duas regras novas:
  1. só se aceitam lotes de líderes com termo >= ao maior termo já visto nessa partição
     (um líder antigo que ainda fale não conta);
  2. um registo é identificado por (partição, seq, termo): o mesmo registo repetido em
     lotes seguintes só gera um alarme.
*/
package raft04;

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
    private final long[] maiorTermo;                     // por partição
    private final Set<String> vistos = new HashSet<>();  // "k#seq#termo"

    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.api = api;
        this.partitioner = new Partitioner(n);
        this.publisher = new IpfsPubSub(api);
        this.vp = new VectorClock(n);
        this.maiorTermo = new long[n];
        this.monitors = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::event);
        }
    }

    public void start() {
        for (int k = 0; k < n; k++) {
            final int part = k;
            Thread t = new Thread(() -> listen(part));
            t.setDaemon(true);
            t.start();
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
        System.out.println("Processador de alarmes (N=" + n + ", LIMIAR_ALARME=" + Wire.LIMIAR_ALARME
                + "): subscreve " + n + " partições");
    }

    // subscribe() bloqueia; se o IPFS cair, volta a ligar-se passados 2 s
    private void listen(int k) {
        while (true) {
            try {
                new IpfsPubSub(api).subscribe(Wire.topicData(k), msg -> onBatch(k, msg));
            } catch (Exception e) {
                log.event("ERRO;p" + k + ";subscricao=" + Wire.topicData(k) + ";" + e);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void onBatch(int k, String msg) {
        Batch b;
        try {
            b = Batch.decode(msg, n);   // rejeita vetores de dimensão diferente de N
        } catch (RuntimeException e) {
            System.out.println("REJEITADO: " + (msg == null ? "" : msg.trim()) + " (" + e + ")");
            return;
        }

        // regra 1: lote de líder com termo antigo é ignorado
        synchronized (lock) {
            if (b.term < maiorTermo[k]) {
                log.event("IGNORADO;p" + k + ";lote_termo_antigo;termo=" + b.term
                        + ";maior=" + maiorTermo[k]);
                return;
            }
            maiorTermo[k] = b.term;
        }

        monitors[k].onHeartbeat(b.term, b.leader, b.hb);
        String intervalo = b.records.isEmpty() ? "-"
                : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq();
        log.event("RECEBIDO;p" + k + ";termo=" + b.term + ";lider=n" + b.leader + ";hb=" + b.hb
                + ";ultimoSeq=" + b.lastSeq + ";n=" + b.n + ";seq=" + intervalo);

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
            // regra 2: o mesmo registo (partição, seq, termo) só conta uma vez
            if (!vistos.add(k + "#" + r.getSeq() + "#" + r.getTerm())) {
                return;
            }
            lp = Math.max(lp, r.getLamport());
            vp.merge(r.getVector());
            lSnap = lp;
            vSnap = vp.copy();   // cópia: o vetor partilhado continua a ser alterado por outras threads
        }

        if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
            String alarmSensor = "ALARM-" + r.getSensorId();
            int j = partitioner.of(alarmSensor);
            if (monitors[j].state() != HeartbeatMonitor.State.ATIVO) {
                System.out.println("AVISO: partição p" + j + " sem líder conhecido; alarme "
                        + alarmSensor + " pode perder-se");
            }
            try {
                publisher.publish(Wire.topicIn(j),
                        Wire.in(alarmSensor, r.getTemperature(), lSnap, vSnap));
                System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq()
                        + " (termo " + r.getTerm() + ") → p" + j);
            } catch (Exception e) {
                System.out.println("Falha ao publicar alarme: " + e);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.out.println("Uso: java raft04.AlarmProcessor <N> [api]");
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
        Thread.currentThread().join();   // as threads de subscrição são daemon: mantém o processo vivo
    }
}