package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.Set;

/** Cria ou carrega a chave mestra de 256 bits, que nunca deve ser partilhada com réplicas. */
public final class MasterKey {
    private MasterKey() {}

    public static byte[] carregarOuCriar(Path caminho) throws IOException {
        if (Files.exists(caminho)) {
            byte[] chave = Files.readAllBytes(caminho);
            if (chave.length != 32) {
                throw new IOException("A master.key existente tem " + chave.length + " bytes; eram esperados 32. Arranque recusado.");
            }
            return chave;
        }

        Path pasta = caminho.toAbsolutePath().getParent();
        if (pasta != null) Files.createDirectories(pasta);

        byte[] chave = new byte[32];
        new SecureRandom().nextBytes(chave);

        // CREATE_NEW evita substituir acidentalmente uma chave criada por outro arranque concorrente.
        try {
            Files.write(caminho, chave, java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE);
        } catch (java.nio.file.FileAlreadyExistsException corrida) {
            byte[] existente = Files.readAllBytes(caminho);
            if (existente.length != 32) throw new IOException("A master.key criada em paralelo não tem 32 bytes.", corrida);
            return existente;
        }

        // Em sistemas POSIX, remove permissões para grupo e outros utilizadores.
        // No Windows, as permissões POSIX não existem; nesse caso mantemos as ACLs do sistema.
        try {
            Set<PosixFilePermission> permissoes = EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(caminho, permissoes);
        } catch (UnsupportedOperationException ignored) {
            // Sistema de ficheiros sem suporte para permissões POSIX (por exemplo, configuração Windows comum).
        }
        return chave;
    }
}
