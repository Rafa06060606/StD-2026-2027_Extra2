package tpf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Base64;
import java.util.List;

/** Persiste e abre os chunks cifrados na pasta selados/. */
public final class Sealed {
    private final Path pasta;
    private final byte[] masterKey;

    public Sealed(Path pasta, byte[] masterKey) throws IOException {
        this.pasta = pasta;
        this.masterKey = masterKey.clone();
        Files.createDirectories(pasta);
    }

    /** Cifra e grava o par .bin/.hdr. Os ficheiros são criados sem substituir ficheiros existentes. */
    public String guardar(byte[] dadosComPadding) throws Exception {
        ChunkCrypto.Resultado r = ChunkCrypto.seal(dadosComPadding, masterKey);
        Path bin = pasta.resolve(r.chunkId() + ".bin");
        Path hdr = pasta.resolve(r.chunkId() + ".hdr");
        String cabecalho = r.keyVersion() + ";" + Base64.getEncoder().encodeToString(r.salt()) + ";"
                + Base64.getEncoder().encodeToString(r.nonce()) + System.lineSeparator();

        // CREATE_NEW evita sobrescrever silenciosamente um chunk já existente.
        Files.write(bin, r.ciphertext(), StandardOpenOption.CREATE_NEW);
        try {
            Files.writeString(hdr, cabecalho, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (Exception e) {
            Files.deleteIfExists(bin); // evita deixar um par incompleto se a escrita do cabeçalho falhar
            throw e;
        }
        return r.chunkId();
    }

    /** Lê os dois ficheiros, valida-os criptograficamente e reconstrói os registos. */
    public List<Registo> abrir(String chunkId) throws Exception {
        Path bin = pasta.resolve(chunkId + ".bin");
        Path hdr = pasta.resolve(chunkId + ".hdr");
        byte[] ciphertext = Files.readAllBytes(bin);
        String[] campos = Files.readString(hdr, StandardCharsets.UTF_8).trim().split(";", -1);
        if (campos.length != 3) throw new IOException("Formato .hdr inválido.");
        int versao = Integer.parseInt(campos[0]);
        byte[] salt = Base64.getDecoder().decode(campos[1]);
        byte[] nonce = Base64.getDecoder().decode(campos[2]);
        byte[] padded = ChunkCrypto.open(chunkId, ciphertext, salt, nonce, versao, masterKey);
        byte[] original = removerPadding(padded);
        return Chunk.desserializar(original);
    }

    /** O formato do chunk começa por int quantidade; o comprimento útil é guardado nos 4 bytes iniciais? */
    private static byte[] removerPadding(byte[] padded) throws IOException {
        // A serialização começa com a quantidade, mas não contém o seu comprimento total.
        // Para remover padding sem ambiguidade, o formato guarda o comprimento útil num prefixo externo.
        // Esta versão lê esse prefixo: [int tamanho útil][bytes serializados][padding].
        if (padded.length < 4) throw new IOException("Chunk demasiado pequeno.");
        int tamanho = java.nio.ByteBuffer.wrap(padded, 0, 4).getInt();
        if (tamanho < 4 || tamanho > padded.length - 4) throw new IOException("Comprimento útil inválido.");
        return java.util.Arrays.copyOfRange(padded, 4, 4 + tamanho);
    }

    /** Prepara o formato cifrado com prefixo de comprimento para permitir remover padding. */
    public static byte[] prepararConteudo(byte[] serializado) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + serializado.length);
        buffer.putInt(serializado.length).put(serializado);
        return Chunk.aplicarPadding(buffer.array());
    }
}
