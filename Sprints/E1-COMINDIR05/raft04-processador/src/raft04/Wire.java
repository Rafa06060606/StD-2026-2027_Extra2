package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class Wire {

    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;

    // Portas TCP
    public static final int PORTO_BASE = 7000;

    // Grupo RAFT
    public static final int GRUPO = 3;

    // Configuração de heartbeat
    public static final long PERIODO_HB_MS =
            Long.getLong("raft04.periodo", 2000L);

    public static final int FATOR_TIMEOUT =
            Integer.getInteger("raft04.fator", 3);

    public static final long VERIFICACAO_MS = 200L;

    // Temporizadores de eleição
    public static final long ELEICAO_MIN_MS =
            Long.getLong("raft04.eleicaoMin", 3L * PERIODO_HB_MS);

    public static final long ELEICAO_MAX_MS =
            Long.getLong("raft04.eleicaoMax", 6L * PERIODO_HB_MS);

    // Alarmes
    public static final double LIMIAR_ALARME = 28.0;

    private Wire() { }

    // Tópicos IPFS

    public static String topicIn(int k) {
        return "raft04-in-p" + k;
    }

    public static String topicData(int k) {
        return "raft04-p" + k;
    }

    public static String topicElection(int k) {
        return "raft04-eleicao-p" + k;
    }

    // Porto TCP individual de cada nó
    // i = 1, 2 ou 3

    public static int nodePort(int k, int i) {

        if (k < 0 || i < 1 || i > GRUPO) {
            throw new IllegalArgumentException("Identificador de nó ou partição inválido");
        }

        return PORTO_BASE + k * GRUPO + (i - 1);
    }

    // Simulação de partição de rede

    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    // IN;<sensor>;<temperatura>;<L>;<V>

    public static String in(String sensor, double temp, long lamport, VectorClock v) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + v;
    }

    // Identifica o tipo da mensagem

    public static String type(String msg) {

        String m = msg.trim();
        int i = m.indexOf(';');

        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }

        return m.substring(0, i);
    }

    // Devolve o conteúdo sem o tipo

    public static String body(String msg) {

        String m = msg.trim();

        return m.substring(m.indexOf(';') + 1);
    }
}