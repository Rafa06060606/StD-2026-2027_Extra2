package raft04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {

    public enum State {
        DESCONHECIDO, ATIVO, SUSPEITO
    }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;

    private State state = State.DESCONHECIDO;

    private long lastNanos;

    private long term = -1;
    private int leader = 0;
    private long hb = 0;

    public HeartbeatMonitor(int partition,
                            long periodMs,
                            int factor,
                            Consumer<String> log) {

        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        this.lastNanos = System.nanoTime();
    }

    // Recebe heartbeat com termo e identificador do líder
    public synchronized void onHeartbeat(long term,
                                          int leader,
                                          long hb) {

        // Ignora termos inferiores
        if (term < this.term) {
            return;
        }

        // Novo termo: novo líder
        if (term > this.term) {

            log.accept(
                    "NOVO_LIDER;p" + partition +
                    ";termo=" + term +
                    ";lider=n" + leader
            );

            this.term = term;
            this.leader = leader;
            this.hb = hb;
        }

        else {

            // Mesmo termo, mas outro líder: situação inválida
            if (this.leader != leader) {

                log.accept(
                        "ERRO;p" + partition +
                        ";dois lideres no termo " + term
                );

                return;
            }

            // Ignora heartbeats repetidos ou atrasados
            if (hb <= this.hb) {
                return;
            }

            // Deteta heartbeats perdidos
            if (hb != this.hb + 1) {

                log.accept(
                        "HB_PERDIDO;p" + partition +
                        ";esperado=" + (this.hb + 1) +
                        ";recebido=" + hb
                );
            }

            this.hb = hb;
        }

        if (state == State.SUSPEITO) {

            log.accept(
                    "RETOMA;p" + partition +
                    ";termo=" + term +
                    ";lider=n" + leader
            );
        }

        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }

    // Verifica se o heartbeat expirou
    public synchronized void check() {

        long elapsed = System.nanoTime() - lastNanos;

        if (state != State.SUSPEITO && elapsed > timeoutNanos) {

            state = State.SUSPEITO;

            log.accept(
                    "SUSPEITA;p" + partition +
                    ";silencio_ms=" + elapsed / 1_000_000
            );
        }
    }

    public synchronized State state() {
        return state;
    }
}