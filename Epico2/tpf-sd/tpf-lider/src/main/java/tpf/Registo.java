package tpf;

/** Representa uma mensagem aceite pelo líder e identificada por um LSN único. */
public record Registo(long lsn, long instante, String autor, String canal, String texto) {
    public Registo {
        if (lsn < 1) throw new IllegalArgumentException("O LSN tem de ser >= 1.");
        if (autor == null || canal == null || texto == null) throw new IllegalArgumentException("Os campos do registo não podem ser null.");
    }
}
