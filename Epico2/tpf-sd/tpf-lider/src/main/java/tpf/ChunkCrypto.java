package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;

/** Implementa HKDF-SHA256 e AES-256-GCM conforme os parâmetros do enunciado. */
public final class ChunkCrypto {
    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] AAD_PREFIX = "tpf-v1".getBytes(StandardCharsets.US_ASCII);
    private static final SecureRandom RANDOM = new SecureRandom();

    public record Resultado(String chunkId, byte[] ciphertext, byte[] salt, byte[] nonce, int keyVersion) {}

    /** Cifra os dados (já serializados e com padding) e devolve os metadados necessários para abrir. */
    public static Resultado seal(byte[] dadosComPadding, byte[] masterKey) throws Exception {
        validarMasterKey(masterKey);
        byte[] salt = aleatorios(32);
        byte[] nonce = aleatorios(12);
        int versao = Wire.KEY_VERSION;
        byte[] dek = derivarDek(masterKey, salt);
        byte[] aad = construirAad(versao, salt, nonce);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
        cipher.updateAAD(aad);
        byte[] ciphertext = cipher.doFinal(dadosComPadding); // inclui a tag GCM de 128 bits
        String id = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(ciphertext));
        Arrays.fill(dek, (byte) 0);
        return new Resultado(id, ciphertext, salt, nonce, versao);
    }

    /** Verifica hash, versão e autenticação GCM, por esta ordem, e devolve os bytes com padding. */
    public static byte[] open(String chunkId, byte[] ciphertext, byte[] salt, byte[] nonce,
                              int keyVersion, byte[] masterKey) throws Exception {
        String hashCalculado = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(ciphertext));
        if (!MessageDigest.isEqual(hashCalculado.getBytes(StandardCharsets.US_ASCII), chunkId.getBytes(StandardCharsets.US_ASCII))) {
            throw new SecurityException("O SHA-256 do conteúdo não corresponde ao chunkId.");
        }
        if (keyVersion != Wire.KEY_VERSION) throw new SecurityException("Versão de chave não suportada: " + keyVersion);
        validarMasterKey(masterKey);
        if (salt.length != 32 || nonce.length != 12) throw new SecurityException("Salt ou nonce com tamanho inválido.");

        byte[] dek = derivarDek(masterKey, salt);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
            cipher.updateAAD(construirAad(keyVersion, salt, nonce));
            return cipher.doFinal(ciphertext); // lança AEADBadTagException se a autenticação falhar
        } finally {
            Arrays.fill(dek, (byte) 0);
        }
    }

    /** HKDF-SHA256: Extract = HMAC(salt, MK); Expand de um bloco = HMAC(PRK, info || 0x01). */
    private static byte[] derivarDek(byte[] masterKey, byte[] salt) throws Exception {
        Mac hmac = Mac.getInstance("HmacSHA256");
        hmac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = hmac.doFinal(masterKey);

        hmac.init(new SecretKeySpec(prk, "HmacSHA256"));
        hmac.update(INFO);
        hmac.update((byte) 0x01);
        byte[] dek = hmac.doFinal(); // SHA-256 produz exatamente 32 bytes
        Arrays.fill(prk, (byte) 0);
        return dek;
    }

    private static byte[] construirAad(int versao, byte[] salt, byte[] nonce) {
        ByteBuffer versaoBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(versao);
        ByteBuffer aad = ByteBuffer.allocate(AAD_PREFIX.length + 4 + salt.length + nonce.length);
        aad.put(AAD_PREFIX).put(versaoBytes.array()).put(salt).put(nonce);
        return aad.array();
    }

    private static byte[] aleatorios(int tamanho) {
        byte[] b = new byte[tamanho];
        RANDOM.nextBytes(b);
        return b;
    }

    private static void validarMasterKey(byte[] key) {
        if (key == null || key.length != 32) throw new IllegalArgumentException("A master.key tem de conter exatamente 32 bytes.");
    }
}
