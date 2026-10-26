
// Comparar relógios vetoriais linha 52

package rep03;

import java.util.Arrays;

public final class VectorClock {

    public enum Order {
        BEFORE, AFTER, EQUAL, CONCURRENT
    }

    private final long[] v;

    public VectorClock(int n) {
        this.v = new long[n];
    }

    private VectorClock(long[] v) {
        this.v = v;
    }

    // Atualiza cada componente com o máximo dos dois vetores.
    public void merge(VectorClock other) {
        if (v.length != other.v.length) {
            throw new IllegalArgumentException(
                "Os vetores têm dimensões diferentes."
            );
        }

        for (int i = 0; i < v.length; i++) {
            v[i] = Math.max(v[i], other.v[i]);
        }
    }

    // Regista um evento local na partição k.
    public void tick(int k) {
        v[k]++;
    }

    public long get(int k) {
        return v[k];
    }

    // Devolve uma cópia independente do vetor.
    public VectorClock copy() {
        return new VectorClock(v.clone());
    }

    // Determina a relação entre este vetor e outro.
    public Order compare(VectorClock other) { 
        if (v.length != other.v.length) {  // Os vetores têm de ter o mesmo número de componentes.
            throw new IllegalArgumentException(
                "Os vetores têm dimensões diferentes."
            );
        }

        boolean menor = false;
        boolean maior = false;

        for (int i = 0; i < v.length; i++) { // Compara componente a componente.
            if (v[i] < other.v[i]) {
                menor = true;
            }

            if (v[i] > other.v[i]) {
                maior = true;
            }
        }

        if (menor && maior) { // Se cada vetor for maior em pelo menos uma componente os acontecimentos são concorrentes.
            return Order.CONCURRENT;
        }

        if (menor) {  // O primeiro vetor é anterior ao segundo.
            return Order.BEFORE;
        }

        if (maior) { // O primeiro vetor é posterior ao segundo.
            return Order.AFTER;
        }

        return Order.EQUAL; // Todas as componentes são iguais.
    }

    // Representação: v0,v1,...,vN-1
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < v.length; i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(v[i]);
        }

        return sb.toString();
    }

    public static VectorClock parse(String s, int n) {
        String[] f = s.trim().split(",");

        if (f.length != n) {
            throw new IllegalArgumentException(
                "Vetor com dimensão " + f.length
                + ", esperado " + n + ": " + s
            );
        }

        long[] v = new long[n];

        for (int i = 0; i < n; i++) {
            v[i] = Long.parseLong(f[i]);
        }

        return new VectorClock(v);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof VectorClock
            && Arrays.equals(v, ((VectorClock) o).v);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(v);
    }
}
