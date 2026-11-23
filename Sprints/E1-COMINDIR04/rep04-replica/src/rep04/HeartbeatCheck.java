package rep04;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

// Uso: java rep04.HeartbeatCheck <N> <pasta> [falhas.txt]
public class HeartbeatCheck {

    private static final class Hb { long inc, hb, ts, last, n, bytes; }
    private static final class Ev { String who; long ts; String type; int k; String detail; }

    private static String f1(double d) { return String.format(Locale.ROOT, "%.1f", d); }
    private static String f2(double d) { return String.format(Locale.ROOT, "%.2f", d); }

    private static String get(String detail, String key) {
        for (String t : detail.split(";")) {
            int i = t.indexOf('=');
            if (i > 0 && t.substring(0, i).equals(key)) return t.substring(i + 1);
        }
        return null;
    }

    private static List<String> lines(File f) throws Exception {
        List<String> out = new ArrayList<>();
        for (String l : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
            if (!l.isBlank()) out.add(l.trim());
        }
        return out;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: java rep04.HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        File dir = new File(args[1]);
        if (!dir.isDirectory()) {
            System.out.println("Erro: pasta não encontrada: " + dir.getPath());
            return;
        }
        File falhasFile = null;
        if (args.length > 2) {
            falhasFile = new File(args[2]);
            if (!falhasFile.exists()) falhasFile = new File(dir, args[2]);
        }

        System.out.println("=== HeartbeatCheck (N = " + n + ", pasta = " + dir.getPath() + ", Δ = "
                + Wire.PERIODO_HB_MS + " ms, TIMEOUT = " + Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT + " ms) ===");

        // ---------- leitura de hb-p<k>.txt ----------
        List<List<Hb>> hbs = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            List<Hb> list = new ArrayList<>();
            hbs.add(list);
            File f = new File(dir, "hb-p" + k + ".txt");
            if (!f.exists()) continue;
            for (String l : lines(f)) {
                String[] p = l.split(";");
                if (p.length != 6) continue;
                Hb h = new Hb();
                h.inc = Long.parseLong(p[0]); h.hb = Long.parseLong(p[1]); h.ts = Long.parseLong(p[2]);
                h.last = Long.parseLong(p[3]); h.n = Long.parseLong(p[4]); h.bytes = Long.parseLong(p[5]);
                list.add(h);
            }
            list.sort(Comparator.comparingLong(h -> h.ts));
        }

        // ---------- 1. batimentos por partição e encarnação ----------
        System.out.println("\n--- 1. Lotes por partição e encarnação ---");
        long totalLotes = 0, totalRegs = 0, totalBytes = 0;
        for (int k = 0; k < n; k++) {
            Map<Long, List<Hb>> byInc = new LinkedHashMap<>();
            for (Hb h : hbs.get(k)) byInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            if (byInc.isEmpty()) System.out.println("p" + k + ": sem hb-p" + k + ".txt");
            for (Map.Entry<Long, List<Hb>> e : byInc.entrySet()) {
                List<Hb> l = e.getValue();
                long vazios = 0, regs = 0, maxN = 0;
                for (Hb h : l) {
                    if (h.n == 0) vazios++;
                    regs += h.n;
                    maxN = Math.max(maxN, h.n);
                    totalBytes += h.bytes;
                }
                totalLotes += l.size();
                totalRegs += regs;
                double somaInt = 0; long maxInt = 0;
                for (int i = 1; i < l.size(); i++) {
                    long d = l.get(i).ts - l.get(i - 1).ts;
                    somaInt += d;
                    maxInt = Math.max(maxInt, d);
                }
                System.out.println("p" + k + " inc=" + e.getKey() + ": " + l.size() + " lotes | vazios " + vazios
                        + " (" + f1(100.0 * vazios / l.size()) + "%) | registos/lote média " + f2((double) regs / l.size())
                        + " máx " + maxN + " | intervalo média "
                        + (l.size() > 1 ? f1(somaInt / (l.size() - 1)) : "n/d") + " ms máx "
                        + (l.size() > 1 ? maxInt : "n/d") + " ms");
            }
        }

        // ---------- leitura dos monitor-*.txt ----------
        List<Ev> evs = new ArrayList<>();
        File[] mons = dir.listFiles((d, name) -> name.matches("monitor-.*\\.txt"));
        if (mons == null) mons = new File[0];
        Arrays.sort(mons, Comparator.comparing(File::getName));
        List<String> participantes = new ArrayList<>();
        for (File f : mons) {
            String who = f.getName().substring("monitor-".length(), f.getName().length() - ".txt".length());
            participantes.add(who);
            for (String l : lines(f)) {
                String[] p = l.split(";", 4);
                if (p.length < 3) continue;
                try {
                    Ev e = new Ev();
                    e.who = who; e.ts = Long.parseLong(p[0]); e.type = p[1];
                    e.k = Integer.parseInt(p[2].substring(1));
                    e.detail = p.length > 3 ? p[3] : "";
                    evs.add(e);
                } catch (RuntimeException ex) { /* linha ignorada */ }
            }
        }
        evs.sort(Comparator.comparingLong(e -> e.ts));

        // ---------- 2. suspeitas: falha real vs. falsa suspeita ----------
        System.out.println("\n--- 2. Suspeitas ---");
        int reais = 0, falsas = 0;
        for (Ev s : evs) {
            if (!s.type.equals("SUSPEITA")) continue;
            Hb ultimo = null;
            if (s.k < n) for (Hb h : hbs.get(s.k)) if (h.ts <= s.ts) ultimo = h;
            String silencio = ultimo == null ? "n/d" : (s.ts - ultimo.ts) + " ms";
            Ev retoma = null;
            for (Ev r : evs) {
                if (r.ts > s.ts && r.type.equals("RETOMA") && r.k == s.k && r.who.equals(s.who)) { retoma = r; break; }
            }
            boolean falsa = retoma != null && ultimo != null && String.valueOf(ultimo.inc).equals(get(retoma.detail, "inc"));
            if (falsa) falsas++; else reais++;
            System.out.println(s.who + " p" + s.k + " @" + s.ts + ": " + (falsa ? "FALSA SUSPEITA" : "FALHA REAL")
                    + " | silêncio desde o último heartbeat publicado: " + silencio
                    + (falsa ? " | suspeito durante " + (retoma.ts - s.ts) + " ms" : "")
                    + (!falsa && retoma != null ? " | retomou com nova encarnação" : ""));
        }
        System.out.println("Total: " + reais + " falhas reais, " + falsas + " falsas suspeitas");

        // ---------- 3. tempo de deteção (falhas.txt) ----------
        if (falhasFile != null && falhasFile.exists()) {
            System.out.println("\n--- 3. Tempo de deteção (" + falhasFile.getPath() + ") ---");
            long lo = Wire.PERIODO_HB_MS * (Wire.FATOR_TIMEOUT - 1), hi = Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT + Wire.VERIFICACAO_MS;
            List<long[]> falhas = new ArrayList<>();   // {k, instante}
            for (String l : lines(falhasFile)) {
                String[] p = l.split(";");
                if (p.length == 2) falhas.add(new long[] { Integer.parseInt(p[0].trim().substring(1)), Long.parseLong(p[1].trim()) });
            }
            double soma = 0, min = Double.MAX_VALUE, max = 0; int cnt = 0;
            for (long[] fa : falhas) {
                long limite = Long.MAX_VALUE;
                for (long[] o : falhas) if (o[0] == fa[0] && o[1] > fa[1]) limite = Math.min(limite, o[1]);
                for (String who : participantes) {
                    for (Ev s : evs) {
                        if (s.type.equals("SUSPEITA") && s.who.equals(who) && s.k == fa[0] && s.ts >= fa[1] && s.ts < limite) {
                            double d = (s.ts - fa[1]) / 1000.0;
                            boolean dentro = s.ts - fa[1] > lo && s.ts - fa[1] <= hi;
                            System.out.println("falha p" + fa[0] + " @" + fa[1] + " → " + who + ": " + f2(d) + " s"
                                    + (dentro ? " (dentro de ]" + lo / 1000.0 + ", " + hi / 1000.0 + "] s)" : " (FORA de ]" + lo / 1000.0 + ", " + hi / 1000.0 + "] s)"));
                            soma += d; min = Math.min(min, d); max = Math.max(max, d); cnt++;
                            break;
                        }
                    }
                }
            }
            if (cnt > 0) System.out.println("Deteção: mínimo " + f2(min) + " s | médio " + f2(soma / cnt) + " s | máximo " + f2(max) + " s (" + cnt + " observações)");
            else System.out.println("Nenhuma suspeita associada às falhas indicadas.");
        }

        // ---------- 4. latência de lote ----------
        System.out.println("\n--- 4. Latência de lote (RECEBIDO mais cedo que contém o registo − instante do registo) ---");
        System.out.println("(os instantes dos registos têm resolução de 1 s, por isso a latência fica sobrestimada em ~0,5 s em média)");
        long totalRegsLider = 0, bytesReg = 0;
        for (int k = 0; k < n; k++) {
            File f = new File(dir, "lider-p" + k + ".txt");
            if (!f.exists()) continue;
            List<long[]> ranges = new ArrayList<>();   // {de, ate, ts}
            for (Ev e : evs) {
                if (e.k != k || !e.type.equals("RECEBIDO")) continue;
                String s = get(e.detail, "seq");
                if (s == null || s.equals("-")) continue;
                String[] ab = s.split("\\.\\.");
                ranges.add(new long[] { Long.parseLong(ab[0]), Long.parseLong(ab[1]), e.ts });
            }
            double soma = 0, max = 0; long cnt = 0, sem = 0;
            for (String l : lines(f)) {
                SensorRecord r = SensorRecord.fromLine(l);
                totalRegsLider++;
                bytesReg += ("REG;127.0.0.1;" + (Wire.PORTO_BASE + k) + ";" + l).getBytes(StandardCharsets.UTF_8).length;
                long rec = Long.MAX_VALUE;
                for (long[] rg : ranges) if (r.getSeq() >= rg[0] && r.getSeq() <= rg[1]) rec = Math.min(rec, rg[2]);
                if (rec == Long.MAX_VALUE) { sem++; continue; }
                long t0 = LocalDateTime.parse(r.getTimestamp()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                double lat = rec - t0;
                soma += lat; max = Math.max(max, lat); cnt++;
            }
            System.out.println("p" + k + ": " + cnt + " registos | latência média " + (cnt > 0 ? f1(soma / cnt) : "n/d")
                    + " ms | máxima " + (cnt > 0 ? f1(max) : "n/d") + " ms" + (sem > 0 ? " | sem RECEBIDO: " + sem : ""));
        }

        // ---------- 5. HB_PERDIDO, REINICIO e RECUPERACAO ----------
        System.out.println("\n--- 5. Eventos do detetor ---");
        for (String who : participantes) {
            int perdidos = 0, recup = 0, reinicios = 0, retomas = 0;
            for (Ev e : evs) {
                if (!e.who.equals(who)) continue;
                switch (e.type) {
                    case "HB_PERDIDO": perdidos++; break;
                    case "RECUPERACAO": recup++; break;
                    case "REINICIO": reinicios++; break;
                    case "RETOMA": retomas++; break;
                    default:
                }
            }
            System.out.println(who + ": HB_PERDIDO " + perdidos + " | RECUPERACAO " + recup + " | REINICIO " + reinicios + " | RETOMA " + retomas);
        }

        // ---------- 6. custo em bytes ----------
        System.out.println("\n--- 6. Mensagens e bytes por registo ---");
        System.out.println("Mensagens no tópico de dados: COMINDIR03 (um REG por registo) = " + totalRegsLider
                + " | COMINDIR04 (um LOTE por intervalo) = " + totalLotes);
        if (totalRegs > 0) System.out.println("Bytes por registo em LOTE (cabeçalhos de todos os lotes incluídos): " + f1((double) totalBytes / totalRegs));
        if (totalRegsLider > 0) System.out.println("Bytes por registo num REG equivalente da COMINDIR03: " + f1((double) bytesReg / totalRegsLider));
    }
}
