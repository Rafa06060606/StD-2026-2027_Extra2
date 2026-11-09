package raft04;

import java.time.LocalDateTime;

public class SensorRecord {

    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;
    private final long term;

    public SensorRecord(long seq, long term, String sensorId,
                        double temperature, String timestamp,
                        long lamport, VectorClock vector) {

        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    public static SensorRecord now(long seq, long term, String sensorId,
                                   double temperature, long lamport,
                                   VectorClock vector) {

        return new SensorRecord(
                seq,
                term,
                sensorId,
                temperature,
                LocalDateTime.now().withNano(0).toString(),
                lamport,
                vector
        );
    }

    // seq;termo;sensor;temperatura;instante;L;V
    public String toLine() {

        return seq + ";" + term + ";" + sensorId + ";" +
                temperature + ";" + timestamp + ";" +
                lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line) {
        return parse(line, -1);
    }

    public static SensorRecord fromLine(String line, int n) {
        return parse(line, n);
    }

    private static SensorRecord parse(String line, int n) {

        String[] f = line.trim().split(";");

        if (f.length != 7) {
            throw new IllegalArgumentException(
                    "Registo mal formado: " + line
            );
        }

        int dim = n >= 0 ? n : f[6].split(",").length;

        return new SensorRecord(
                Long.parseLong(f[0]),
                Long.parseLong(f[1]),
                f[2],
                Double.parseDouble(f[3]),
                f[4],
                Long.parseLong(f[5]),
                VectorClock.parse(f[6], dim)
        );
    }

    public long getSeq() {
        return seq;
    }

    public long getTerm() {
        return term;
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
        return vector;
    }
}