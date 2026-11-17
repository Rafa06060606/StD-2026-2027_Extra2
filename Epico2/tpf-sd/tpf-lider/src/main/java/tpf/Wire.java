package tpf;

/**
 * Centraliza os parâmetros de comunicação e armazenamento usados no Sprint E2-02.
 * Os valores podem ser alterados no arranque através de propriedades -Dtpf.*.
 */
public final class Wire {
    // Tamanho máximo do conteúdo serializado de um chunk, antes do padding.
    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunk.maxBytes", 65_536);

    // O conteúdo serializado é completado até um múltiplo deste valor.
    public static final int PAD_BYTES = Integer.getInteger("tpf.pad.bytes", 4_096);

    // Tempo máximo, em milissegundos, desde o primeiro registo do chunk aberto.
    public static final long SELAGEM_MS = Long.getLong("tpf.selagem.ms", 5_000L);

    // Versão do formato criptográfico definido no enunciado.
    public static final int KEY_VERSION = 1;

    private Wire() { /* Impede a criação de objetos desta classe utilitária. */ }

    /** Impede que configurações inválidas provoquem comportamentos inesperados. */
    public static void validarConfiguracao() {
        if (CHUNK_MAX_BYTES <= 0) throw new IllegalArgumentException("tpf.chunk.maxBytes tem de ser positivo.");
        if (PAD_BYTES <= 0) throw new IllegalArgumentException("tpf.pad.bytes tem de ser positivo.");
        if (SELAGEM_MS <= 0) throw new IllegalArgumentException("tpf.selagem.ms tem de ser positivo.");
    }
}
