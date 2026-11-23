package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

// Escreve os eventos do detetor na consola e em monitor-<id>.txt, precedidos de currentTimeMillis().
// Formato da linha: instante;evento;p<k>;detalhe
public final class MonitorLog {
    private final Path path;
    private final Object lock = new Object();

    public MonitorLog(String id) {
        this.path = Paths.get("monitor-" + id + ".txt");
    }

    public void event(String e) {
        String line = System.currentTimeMillis() + ";" + e;
        synchronized (lock) {
            System.out.println("MONITOR: " + line);
            try {
                Files.writeString(path, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException ex) {
                System.out.println("Erro a escrever " + path + ": " + ex.getMessage());
            }
        }
    }
}