package tpf;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Agrupa registos que serão serializados e cifrados em conjunto. */
public final class Chunk {
    private final List<Registo> registos = new ArrayList<>();
    private final long instanteAbertura;

    public Chunk(long instanteAbertura) { this.instanteAbertura = instanteAbertura; }

    public long getInstanteAbertura() { return instanteAbertura; }
    public boolean estaVazio() { return registos.isEmpty(); }
    public List<Registo> getRegistos() { return List.copyOf(registos); }
    public long primeiroLsn() { return registos.get(0).lsn(); }
    public long ultimoLsn() { return registos.get(registos.size() - 1).lsn(); }

    /** Acrescenta um registo ao chunk ainda aberto. */
    public void adicionar(Registo registo) { registos.add(registo); }

    /**
     * Formato binário documentado no README:
     * int quantidade; para cada registo: long lsn, long instante,
     * e para cada String: int comprimento + bytes UTF-8.
     */
    public byte[] serializar() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(registos.size());
            for (Registo r : registos) {
                out.writeLong(r.lsn());
                out.writeLong(r.instante());
                escreverString(out, r.autor());
                escreverString(out, r.canal());
                escreverString(out, r.texto());
            }
        }
        return bytes.toByteArray();
    }

    /** Reconstrói os registos a partir dos bytes úteis, sem o padding. */
    public static List<Registo> desserializar(byte[] dados) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(dados))) {
            int quantidade = in.readInt();
            if (quantidade < 0 || quantidade > 1_000_000) throw new IOException("Quantidade de registos inválida no chunk.");
            List<Registo> resultado = new ArrayList<>();
            for (int i = 0; i < quantidade; i++) {
                long lsn = in.readLong();
                long instante = in.readLong();
                String autor = lerString(in);
                String canal = lerString(in);
                String texto = lerString(in);
                resultado.add(new Registo(lsn, instante, autor, canal, texto));
            }
            // Bytes restantes não são aceites: evita ignorar dados corrompidos no conteúdo útil.
            if (in.available() != 0) throw new IOException("Existem bytes inesperados depois dos registos.");
            return resultado;
        } catch (EOFException e) {
            throw new IOException("Chunk incompleto ou corrompido.", e);
        }
    }

    private static void escreverString(DataOutputStream out, String valor) throws IOException {
        byte[] dados = valor.getBytes(StandardCharsets.UTF_8);
        out.writeInt(dados.length);
        out.write(dados);
    }

    private static String lerString(DataInputStream in) throws IOException {
        int tamanho = in.readInt();
        if (tamanho < 0 || tamanho > in.available()) throw new IOException("Comprimento de texto inválido no chunk.");
        byte[] dados = in.readNBytes(tamanho);
        if (dados.length != tamanho) throw new EOFException();
        return new String(dados, StandardCharsets.UTF_8);
    }

    /** Acrescenta padding aleatório até ao múltiplo seguinte de PAD_BYTES. */
    public static byte[] aplicarPadding(byte[] original) {
        int tamanhoFinal = ((original.length + Wire.PAD_BYTES - 1) / Wire.PAD_BYTES) * Wire.PAD_BYTES;
        // Mesmo que os dados já sejam múltiplos, não acrescentamos um bloco desnecessário.
        byte[] resultado = new byte[tamanhoFinal];
        System.arraycopy(original, 0, resultado, 0, original.length);
        if (tamanhoFinal > original.length) {
            byte[] padding = new byte[tamanhoFinal - original.length];
            new java.security.SecureRandom().nextBytes(padding);
            System.arraycopy(padding, 0, resultado, original.length, padding.length);
        }
        return resultado;
    }
}
