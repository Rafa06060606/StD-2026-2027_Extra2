// package tpf;

// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.io.TempDir;
// import java.nio.file.Path;
// import java.util.List;
// import static org.junit.jupiter.api.Assertions.*;

// /** Testa o percurso completo: registos -> serialização -> padding -> cifra -> ficheiros -> abertura. */
// class SealedTest {
//     @TempDir Path pasta;

//     @Test void guardarEAbrirRecuperaOsRegistosOriginais() throws Exception {
//         byte[] master = new byte[32];
//         Registo r1 = new Registo(1, 1000, "Ana", "arquivo", "Olá; mundo");
//         Registo r2 = new Registo(2, 2000, "João", "arquivo", "segunda mensagem\ncom linha nova");

//         Chunk chunk = new Chunk(900);
//         chunk.adicionar(r1);
//         chunk.adicionar(r2);

//         Sealed sealed = new Sealed(pasta.resolve("selados"), master);
//         String id = sealed.guardar(Sealed.prepararConteudo(chunk.serializar()));
//         List<Registo> abertos = sealed.abrir(id);

//         assertEquals(List.of(r1, r2), abertos);
//         assertTrue(java.nio.file.Files.exists(pasta.resolve("selados").resolve(id + ".bin")));
//         assertTrue(java.nio.file.Files.exists(pasta.resolve("selados").resolve(id + ".hdr")));
//     }
// }

package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SealedTest {

    @TempDir
    Path pastaTemporaria;

    @Test
    void deveGuardarAbrirEReconstruirRegistos() throws Exception {

        byte[] masterKey = new byte[32];

        for (int i = 0; i < masterKey.length; i++) {
            masterKey[i] = (byte) (i + 1);
        }

        Chunk chunk = new Chunk(System.currentTimeMillis());

        Registo r1 = new Registo(
                1, 1000, "Matilde", "arquivo", "Primeira mensagem"
        );

        Registo r2 = new Registo(
                2, 2000, "Utilizador2", "arquivo", "Segunda mensagem"
        );

        chunk.adicionar(r1);
        chunk.adicionar(r2);

        Sealed sealed = new Sealed(
                pastaTemporaria.resolve("selados"),
                masterKey
        );

        byte[] serializado = chunk.serializar();

        byte[] comPadding = Sealed.prepararConteudo(serializado);

        String chunkId = sealed.guardar(comPadding);

        List<Registo> recuperados = sealed.abrir(chunkId);

        assertEquals(2, recuperados.size());

        assertEquals(r1, recuperados.get(0));
        assertEquals(r2, recuperados.get(1));

        assertTrue(
                java.nio.file.Files.exists(
                        pastaTemporaria.resolve("selados").resolve(chunkId + ".bin")
                )
        );

        assertTrue(
                java.nio.file.Files.exists(
                        pastaTemporaria.resolve("selados").resolve(chunkId + ".hdr")
                )
        );
    }
}
