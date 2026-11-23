package rep03;

import java.time.LocalDateTime;

public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, String sensorId, double temperature,
                        String timestamp, long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = (vector != null) ? vector.copy() : null;
    }

    // Cria um registo com L=0 e um vetor da dimensão N.
    public static SensorRecord now(long seq, String sensorId,
                                    double temperature, int n) {
        return new SensorRecord(
            seq,
            sensorId,
            temperature,
            LocalDateTime.now().withNano(0).toString(),
            0,
            new VectorClock(n)
        );
    }

    // Formato: seq;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";"
            + sensorId + ";"
            + temperature + ";"
            + timestamp + ";"
            + lamport + ";"
            + (vector != null ? vector.toString() : "");
    }

    public static SensorRecord fromLine(String line) {
        // Usar limit 6 evita problemas caso o vector.toString() contenha ponto e vírgula
        String[] f = line.trim().split(";", 6);

        if (f.length != 6) {
            throw new IllegalArgumentException(
                "Registo mal formado: " + line
            );
        }

        long seq = Long.parseLong(f[0]);
        String sensorId = f[1];
        double temperature = Double.parseDouble(f[2]);
        String timestamp = f[3];
        long lamport = Long.parseLong(f[4]);
        
        if (f[5].isBlank()) {
            throw new IllegalArgumentException(
                "Relógio vetorial vazio: " + line
            );
        }

        int n = f[5].split(",").length;
        VectorClock vector = VectorClock.parse(f[5], n);

        return new SensorRecord(
            seq, sensorId, temperature, timestamp, lamport, vector
        );
    }

    public long getSeq() {
        return seq;
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public long getLamport() {
        return lamport;
    }

    public VectorClock getVector() {
        return vector != null ? vector.copy() : null;
    }
}