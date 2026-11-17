// package tpf;

// import org.junit.jupiter.api.Test;
// import java.util.Arrays;
// import static org.junit.jupiter.api.Assertions.*;

// /** Testes pedidos no Sprint E2-02 para confidencialidade e deteção de alterações. */
// class ChunkCryptoTest {
//     private final byte[] master = new byte[32];
//     private final byte[] dados = "mensagem secreta de teste".getBytes(java.nio.charset.StandardCharsets.UTF_8);

//     @Test void cifrarEDecifrarDevolveOsMesmosBytes() throws Exception {
//         ChunkCrypto.Resultado r = ChunkCrypto.seal(dados, master);
//         byte[] aberto = ChunkCrypto.open(r.chunkId(), r.ciphertext(), r.salt(), r.nonce(), r.keyVersion(), master);
//         assertArrayEquals(dados, aberto);
//     }

//     @Test void alterarCiphertextFazFalhar() throws Exception {
//         ChunkCrypto.Resultado r = ChunkCrypto.seal(dados, master);
//         byte[] alterado = r.ciphertext().clone(); alterado[0] ^= 1;
//         assertThrows(SecurityException.class, () -> ChunkCrypto.open(r.chunkId(), alterado, r.salt(), r.nonce(), r.keyVersion(), master));
//     }

//     @Test void alterarSaltFazFalhar() throws Exception {
//         ChunkCrypto.Resultado r = ChunkCrypto.seal(dados, master);
//         byte[] salt = r.salt().clone(); salt[0] ^= 1;
//         assertThrows(Exception.class, () -> ChunkCrypto.open(r.chunkId(), r.ciphertext(), salt, r.nonce(), r.keyVersion(), master));
//     }

//     @Test void alterarNonceFazFalhar() throws Exception {
//         ChunkCrypto.Resultado r = ChunkCrypto.seal(dados, master);
//         byte[] nonce = r.nonce().clone(); nonce[0] ^= 1;
//         assertThrows(Exception.class, () -> ChunkCrypto.open(r.chunkId(), r.ciphertext(), r.salt(), nonce, r.keyVersion(), master));
//     }

//     @Test void alterarVersaoFazFalhar() throws Exception {
//         ChunkCrypto.Resultado r = ChunkCrypto.seal(dados, master);
//         assertThrows(SecurityException.class, () -> ChunkCrypto.open(r.chunkId(), r.ciphertext(), r.salt(), r.nonce(), 2, master));
//     }

//     @Test void usarOutraMasterKeyFazFalhar() throws Exception {
//         ChunkCrypto.Resultado r = ChunkCrypto.seal(dados, master);
//         byte[] outra = new byte[32]; Arrays.fill(outra, (byte) 7);
//         assertThrows(Exception.class, () -> ChunkCrypto.open(r.chunkId(), r.ciphertext(), r.salt(), r.nonce(), r.keyVersion(), outra));
//     }
// }

package tpf;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class ChunkCryptoTest {

    private byte[] criarMasterKey() {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) 7);
        return key;
    }

    private String calcularHash(byte[] dados) throws Exception {
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(dados);
        return HexFormat.of().formatHex(hash);
    }

    @Test
    void deveCifrarEDecifrarCorretamente() throws Exception {

        byte[] masterKey = criarMasterKey();

        byte[] original = "Mensagem secreta de teste".getBytes();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal(original, masterKey);

        byte[] recuperado = ChunkCrypto.open(
                resultado.chunkId(),
                resultado.ciphertext(),
                resultado.salt(),
                resultado.nonce(),
                resultado.keyVersion(),
                masterKey
        );

        assertArrayEquals(original, recuperado);
    }

    @Test
    void deveDetetarAlteracaoDoCiphertextPeloHash() throws Exception {

        byte[] masterKey = criarMasterKey();
        byte[] original = "Teste de integridade".getBytes();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal(original, masterKey);

        byte[] alterado = resultado.ciphertext().clone();
        alterado[0] ^= 1;

        assertThrows(SecurityException.class, () ->
                ChunkCrypto.open(
                        resultado.chunkId(),
                        alterado,
                        resultado.salt(),
                        resultado.nonce(),
                        resultado.keyVersion(),
                        masterKey
                )
        );
    }

    @Test
    void deveDetetarAlteracaoDaAutenticacaoGcm() throws Exception {

        byte[] masterKey = criarMasterKey();
        byte[] original = "Teste GCM".getBytes();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal(original, masterKey);

        byte[] alterado = resultado.ciphertext().clone();
        alterado[0] ^= 1;

        // Recalculamos o hash para ultrapassar a primeira verificação.
        // A autenticação GCM deve continuar a detetar a alteração.
        String novoId = calcularHash(alterado);

        assertThrows(Exception.class, () ->
                ChunkCrypto.open(
                        novoId,
                        alterado,
                        resultado.salt(),
                        resultado.nonce(),
                        resultado.keyVersion(),
                        masterKey
                )
        );
    }

    @Test
    void deveDetetarAlteracaoDoSalt() throws Exception {

        byte[] masterKey = criarMasterKey();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal("Teste salt".getBytes(), masterKey);

        byte[] saltAlterado = resultado.salt().clone();
        saltAlterado[0] ^= 1;

        assertThrows(Exception.class, () ->
                ChunkCrypto.open(
                        resultado.chunkId(),
                        resultado.ciphertext(),
                        saltAlterado,
                        resultado.nonce(),
                        resultado.keyVersion(),
                        masterKey
                )
        );
    }

    @Test
    void deveDetetarAlteracaoDoNonce() throws Exception {

        byte[] masterKey = criarMasterKey();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal("Teste nonce".getBytes(), masterKey);

        byte[] nonceAlterado = resultado.nonce().clone();
        nonceAlterado[0] ^= 1;

        assertThrows(Exception.class, () ->
                ChunkCrypto.open(
                        resultado.chunkId(),
                        resultado.ciphertext(),
                        resultado.salt(),
                        nonceAlterado,
                        resultado.keyVersion(),
                        masterKey
                )
        );
    }

    @Test
    void deveRejeitarUmaMasterKeyDiferente() throws Exception {

        byte[] masterKey = criarMasterKey();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal("Teste chave".getBytes(), masterKey);

        byte[] chaveErrada = new byte[32];
        Arrays.fill(chaveErrada, (byte) 9);

        assertThrows(Exception.class, () ->
                ChunkCrypto.open(
                        resultado.chunkId(),
                        resultado.ciphertext(),
                        resultado.salt(),
                        resultado.nonce(),
                        resultado.keyVersion(),
                        chaveErrada
                )
        );
    }

    @Test
    void deveRejeitarUmaVersaoDeChaveDesconhecida() throws Exception {

        byte[] masterKey = criarMasterKey();

        ChunkCrypto.Resultado resultado =
                ChunkCrypto.seal("Teste versão".getBytes(), masterKey);

        assertThrows(SecurityException.class, () ->
                ChunkCrypto.open(
                        resultado.chunkId(),
                        resultado.ciphertext(),
                        resultado.salt(),
                        resultado.nonce(),
                        999,
                        masterKey
                )
        );
    }
}