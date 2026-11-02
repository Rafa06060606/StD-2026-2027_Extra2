package rep04;

import java.time.LocalDateTime;

public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature,
                LocalDateTime.now().withNano(0).toString(), lamport, vector);
    }

    // seq;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    // dimensão do vetor deduzida da própria linha
    public static SensorRecord fromLine(String line) {
        return parse(line, -1);
    }

    // dimensão do vetor imposta: lança IllegalArgumentException se for diferente de n
    public static SensorRecord fromLine(String line, int n) {
        return parse(line, n);
    }

    private static SensorRecord parse(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        int dim = n >= 0 ? n : f[5].split(",").length;
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3],
                Long.parseLong(f[4]), VectorClock.parse(f[5], dim));
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector; }
}