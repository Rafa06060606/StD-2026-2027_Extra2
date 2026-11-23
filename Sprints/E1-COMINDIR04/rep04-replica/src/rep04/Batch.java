package rep04;
 
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
 
// LOTE;<endereço>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
// <seq>;<sensor>;<temperatura>;<instante>;<L>;<V>   (n linhas)
public final class Batch {
    public final String address;
    public final int port;
    public final long inc;
    public final long hb;
    public final long lastSeq;
    public final int n;                       // número de registos declarado no cabeçalho
    public final List<SensorRecord> records;
 
    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this(address, port, inc, hb, lastSeq, records.size(), records);
    }
 
    private Batch(String address, int port, long inc, long hb, long lastSeq, int n, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.n = n;
        this.records = records;
    }
 
    // objeto → mensagem de texto
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(';').append(port).append(';').append(inc).append(';')
          .append(hb).append(';').append(lastSeq).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }
 
    // só o cabeçalho (usado pelo produtor); a lista de registos fica vazia
    public static Batch decodeHeader(String msg) {
        String[] lines = msg.trim().split("\\r?\\n", 2);
        return parseHeader(lines[0], msg);
    }
 
    // mensagem → objeto; dim = dimensão do vetor (N), ou -1 para a deduzir de cada linha.
    // Valida que o número de linhas de registo coincide com o n do cabeçalho.
    public static Batch decode(String msg, int dim) {
        String[] lines = msg.trim().split("\\r?\\n");
        Batch h = parseHeader(lines[0], msg);
        List<SensorRecord> recs = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) continue;
            recs.add(dim >= 0 ? SensorRecord.fromLine(lines[i], dim) : SensorRecord.fromLine(lines[i]));
        }
        if (recs.size() != h.n) {
            throw new IllegalArgumentException("LOTE com " + recs.size() + " registos, cabeçalho indica " + h.n);
        }
        return new Batch(h.address, h.port, h.inc, h.hb, h.lastSeq, h.n, recs);
    }
 
    private static Batch parseHeader(String header, String msg) {
        String[] f = header.trim().split(";");
        if (f.length != 7 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("LOTE mal formado: " + msg);
        }
        return new Batch(f[1], Integer.parseInt(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
                Long.parseLong(f[5]), Integer.parseInt(f[6]), Collections.emptyList());
    }
}