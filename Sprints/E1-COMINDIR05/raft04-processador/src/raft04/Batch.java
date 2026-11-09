package raft04;
 
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
 
// LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>
// <seq>;<termo>;<sensor>;<temperatura>;<instante>;<L>;<V>
public final class Batch {

    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long lastSeq;
    public final long lastTerm;
    public final int n;

    public final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port,
                 long hb, long lastSeq, long lastTerm,
                 List<SensorRecord> records) {

        this(term, leader, address, port, hb, lastSeq,
                lastTerm, records.size(), records);
    }

    private Batch(long term, int leader, String address, int port,
                  long hb, long lastSeq, long lastTerm,
                  int n, List<SensorRecord> records) {

        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.n = n;

        this.records = Collections.unmodifiableList(
                new ArrayList<>(records)
        );
    }

    // Objeto -> mensagem
    public String encode() {

        StringBuilder sb = new StringBuilder();

        sb.append("LOTE;")
                .append(term).append(';')
                .append(leader).append(';')
                .append(address).append(';')
                .append(port).append(';')
                .append(hb).append(';')
                .append(lastSeq).append(';')
                .append(lastTerm).append(';')
                .append(n);

        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }

        return sb.toString();
    }

    // Apenas o cabeçalho
    public static Batch decodeHeader(String msg) {

        String[] lines = msg.trim().split("\\r?\\n", 2);

        return parseHeader(lines[0], msg);
    }

    // Mensagem -> objeto
    public static Batch decode(String msg, int dim) {

        String[] lines = msg.trim().split("\\r?\\n");

        Batch h = parseHeader(lines[0], msg);

        List<SensorRecord> recs = new ArrayList<>();

        for (int i = 1; i < lines.length; i++) {

            if (lines[i].isBlank()) {
                continue;
            }

            recs.add(
                    dim >= 0
                    ? SensorRecord.fromLine(lines[i], dim)
                    : SensorRecord.fromLine(lines[i])
            );
        }

        if (recs.size() != h.n) {

            throw new IllegalArgumentException(
                    "LOTE com " + recs.size() +
                    " registos, cabeçalho indica " + h.n
            );
        }

        return new Batch(
                h.term,
                h.leader,
                h.address,
                h.port,
                h.hb,
                h.lastSeq,
                h.lastTerm,
                h.n,
                recs
        );
    }

    private static Batch parseHeader(String header, String msg) {

        String[] f = header.trim().split(";");

        if (f.length != 9 || !f[0].equals("LOTE")) {

            throw new IllegalArgumentException(
                    "LOTE mal formado: " + msg
            );
        }

        return new Batch(
                Long.parseLong(f[1]),
                Integer.parseInt(f[2]),
                f[3],
                Integer.parseInt(f[4]),
                Long.parseLong(f[5]),
                Long.parseLong(f[6]),
                Long.parseLong(f[7]),
                Integer.parseInt(f[8]),
                Collections.emptyList()
        );
    }
}