package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {

    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    // Lê todos os registos
    public synchronized List<SensorRecord> all() throws IOException {

        List<SensorRecord> records = new ArrayList<>();

        if (!Files.exists(path)) {
            return records;
        }

        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {

            if (!line.isBlank()) {
                records.add(SensorRecord.fromLine(line));
            }
        }

        return records;
    }

    // Devolve os registos de um intervalo
    public synchronized List<SensorRecord> range(long from, long to)
            throws IOException {

        List<SensorRecord> out = new ArrayList<>();

        for (SensorRecord r : all()) {

            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }

        return out;
    }

    // Acrescenta um registo
    public synchronized void append(SensorRecord r) throws IOException {

        Files.writeString(
                path,
                r.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    // Devolve o último registo
    public synchronized SensorRecord last() throws IOException {

        List<SensorRecord> records = all();

        if (records.isEmpty()) {
            return null;
        }

        return records.get(records.size() - 1);
    }

    // Último seq
    public synchronized long lastSeq() throws IOException {

        SensorRecord r = last();

        return r == null ? 0 : r.getSeq();
    }

    // Termo do último registo
    public synchronized long lastTerm() throws IOException {

        SensorRecord r = last();

        return r == null ? 0 : r.getTerm();
    }

    // Primeiro seq do último bloco de termo
    public synchronized long firstSeqOfLastTermBlock() throws IOException {

        List<SensorRecord> records = all();

        if (records.isEmpty()) {
            return 0;
        }

        long lastTerm = records.get(records.size() - 1).getTerm();

        int i = records.size() - 1;

        while (i > 0 &&
                records.get(i - 1).getTerm() == lastTerm) {
            i--;
        }

        return records.get(i).getSeq();
    }

    // Substitui todos os registos a partir de determinado seq
    public synchronized void replaceFrom(long from,
                                         List<SensorRecord> replacement)
            throws IOException {

        List<SensorRecord> result = new ArrayList<>();

        // Preserva os registos anteriores à divergência
        for (SensorRecord r : all()) {

            if (r.getSeq() < from) {
                result.add(r);
            }
        }

        // Adiciona os registos recebidos do líder
        long previousSeq = from - 1;

        for (SensorRecord r : replacement) {

            if (r.getSeq() < from || r.getSeq() <= previousSeq) {
                throw new IllegalArgumentException(
                        "Sequência inválida nos registos recebidos: " + r.getSeq()
                );
            }

            result.add(r);
            previousSeq = r.getSeq();
        }

        writeAtomically(result);
    }

    // Escreve o ficheiro através de um temporário e substituição atómica
    private void writeAtomically(List<SensorRecord> records)
            throws IOException {

        Path tmp = path.resolveSibling(
                path.getFileName() + ".tmp"
        );

        StringBuilder content = new StringBuilder();

        for (SensorRecord r : records) {
            content.append(r.toLine())
                    .append(System.lineSeparator());
        }

        Files.writeString(
                tmp,
                content.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(
                tmp,
                path,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }
}