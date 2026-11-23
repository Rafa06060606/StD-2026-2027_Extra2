package rep02;

public final class Partitioner {
    private final int n;

    public Partitioner(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("O número de partições N deve ser > 0: " + n);
        }
        this.n = n;
    }

    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    public int count() {
        return n;
    }
}