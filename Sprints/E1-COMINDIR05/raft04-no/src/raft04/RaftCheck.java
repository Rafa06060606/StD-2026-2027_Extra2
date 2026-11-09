package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
RaftCheck <N> <pasta> [falhas.txt]

Lê os ficheiros que os processos deixaram numa pasta (no-*, estado-*, hb-*, eventos-*, monitor-*,
truncados-*, produtor.txt) e escreve um relatório. Não altera nada: só lê.
O relatório vai para a consola e para <pasta>/raftcheck-relatorio.txt.
*/
public class RaftCheck {

    // ---------- estruturas ----------
    static final class Ev {                       // uma linha de eventos-p<k>-n<i>.txt
        long t; String nome; String det; int no;
        Ev(long t, String nome, String det, int no) { this.t = t; this.nome = nome; this.det = det; this.no = no; }
        String campo(String chave) {              // valor de "chave=..." no detalhe
            Matcher m = Pattern.compile("(?:^|;)" + chave + "=([^;]*)").matcher(det);
            return m.find() ? m.group(1) : null;
        }
    }
    static final class Hb {                       // uma linha de hb-p<k>-n<i>.txt
        long termo, t, ultimoSeq; int no;
        Hb(long termo, long t, long ultimoSeq, int no) { this.termo = termo; this.t = t; this.ultimoSeq = ultimoSeq; this.no = no; }
    }
    static final class Reg {                      // uma linha de no-p<k>-n<i>.txt: seq;termo;sensor;temp;instante;L;V
        long seq, termo; String sensor; double temp; String linha;
        String chave() { return sensor + "|" + temp; }
    }
    static final class Leitura {                  // uma linha de produtor.txt: instante;p<k>;sensor;temperatura
        long t; int k; String sensor; double temp;
        String chave() { return sensor + "|" + temp; }
    }

    private static final List<String> saida = new ArrayList<>();
    private static void out(String s) { System.out.println(s); saida.add(s); }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Uso: java raft04.RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        Path falhasPath = args.length == 3 ? Paths.get(args[2]) : null;
        if (n < 1 || !Files.isDirectory(pasta)) { System.out.println("N inválido ou pasta inexistente."); return; }

        out("=== RAFT CHECK ===");
        out("Partições: " + n + " | pasta: " + pasta.toAbsolutePath());

        List<Leitura> leituras = lerProdutor(pasta.resolve("produtor.txt"));
        List<String[]> falhas = falhasPath == null ? List.of() : lerFalhas(falhasPath);

        long termosComVariosLideres = 0;
        long truncadosTotal = 0, divergentesTotal = 0;
        for (int k = 0; k < n; k++) {
            out("");
            out("################ p" + k + " ################");
            Map<Integer, List<Ev>> eventos = new TreeMap<>();
            Map<Integer, List<Reg>> registos = new TreeMap<>();
            Map<Integer, List<Hb>> hbs = new TreeMap<>();
            for (int i = 1; i <= 3; i++) {
                Path pe = pasta.resolve("eventos-p" + k + "-n" + i + ".txt");
                Path pr = pasta.resolve("no-p" + k + "-n" + i + ".txt");
                Path ph = pasta.resolve("hb-p" + k + "-n" + i + ".txt");
                if (Files.isRegularFile(pe)) eventos.put(i, lerEventos(pe, i));
                if (Files.isRegularFile(pr)) registos.put(i, lerRegistos(pr));
                if (Files.isRegularFile(ph)) hbs.put(i, lerHb(ph, i));
            }
            if (eventos.isEmpty() && registos.isEmpty()) { out("Sem ficheiros desta partição (nenhum nó correu aqui)."); continue; }

            List<Ev> todos = new ArrayList<>();
            for (List<Ev> l : eventos.values()) todos.addAll(l);
            todos.sort(Comparator.comparingLong(e -> e.t));
            List<Hb> todosHb = new ArrayList<>();
            for (List<Hb> l : hbs.values()) todosHb.addAll(l);
            todosHb.sort(Comparator.comparingLong(h -> h.t));

            termosComVariosLideres += termos(todos);
            seguranca(k, eventos, pasta);
            primeiroLider(todos);
            long[] resFalhas = indisponibilidade(k, falhas, todosHb, todos, leituras);
            long[] tr = truncados(k, todos, registos, pasta);
            truncadosTotal += tr[0];
            lidoFinal(k, todos, registos);
            divergentesTotal += coincidencia(k, registos);
        }
        perdidas(n, leituras, falhas, pasta);
        monitores(pasta);

        out("");
        out("=== RESUMO ===");
        out("Termos com mais de um LIDER (tem de ser 0): " + termosComVariosLideres);
        out("Registos descartados (TRUNCADO): " + truncadosTotal);
        out("seq divergentes na coincidência final: " + divergentesTotal);
        out("=== FIM ===");
        Files.write(pasta.resolve("raftcheck-relatorio.txt"), saida, StandardCharsets.UTF_8);
    }

    // ---------- 1. termos, candidatos, votos, líder ----------
    private static long termos(List<Ev> todos) {
        TreeMap<Long, Set<Integer>> candidatos = new TreeMap<>();
        TreeMap<Long, List<Ev>> lideres = new TreeMap<>();
        TreeMap<Long, Long> primeiroCandidato = new TreeMap<>();
        Map<String, Integer> votos = new HashMap<>();           // "termo#candidato" -> votos concedidos por outros
        for (Ev e : todos) {
            if (e.nome.equals("CANDIDATO")) {
                long t = Long.parseLong(e.campo("termo"));
                candidatos.computeIfAbsent(t, x -> new TreeSet<>()).add(e.no);
                primeiroCandidato.merge(t, e.t, Math::min);
            } else if (e.nome.equals("LIDER")) {
                lideres.computeIfAbsent(Long.parseLong(e.campo("termo")), x -> new ArrayList<>()).add(e);
            } else if (e.nome.equals("VOTO") && "concedido".equals(e.det.substring(e.det.lastIndexOf(';') + 1))) {
                String c = e.campo("candidato");               // "n2"
                votos.merge(e.campo("termo") + "#" + c, 1, Integer::sum);
            }
        }
        TreeSet<Long> ts = new TreeSet<>(candidatos.keySet());
        ts.addAll(lideres.keySet());
        out("-- Termos (candidatos, votos, líder, duração da eleição) --");
        long varios = 0;
        for (long t : ts) {
            Set<Integer> cs = candidatos.getOrDefault(t, Set.of());
            StringBuilder sb = new StringBuilder("termo " + t + ": candidatos ");
            if (cs.isEmpty()) sb.append("-");
            for (int c : cs) {
                int v = 1 + votos.getOrDefault(t + "#n" + c, 0);   // o próprio voto + os concedidos
                sb.append("n").append(c).append("(").append(v).append(" voto").append(v == 1 ? "" : "s").append(") ");
            }
            List<Ev> ls = lideres.get(t);
            if (ls == null) {
                sb.append("→ ").append(cs.size() >= 2 ? "VOTO DIVIDIDO" : "sem maioria (1 candidato)").append(", sem líder");
            } else {
                Set<Integer> quem = new TreeSet<>();
                for (Ev e : ls) quem.add(e.no);
                sb.append("→ líder ");
                for (int q : quem) sb.append("n").append(q).append(" ");
                if (quem.size() > 1) { sb.append("[ERRO: ").append(quem.size()).append(" líderes no mesmo termo]"); varios++; }
                long ini = primeiroCandidato.getOrDefault(t, ls.get(0).t);
                sb.append("| eleição ").append(ls.get(0).t - ini).append(" ms");
            }
            out(sb.toString());
        }
        return varios;
    }

    // ---------- 2. segurança: o termo de cada nó nunca diminui ----------
    private static void seguranca(int k, Map<Integer, List<Ev>> eventos, Path pasta) throws IOException {
        out("-- Segurança --");
        boolean tudoOk = true;
        for (Map.Entry<Integer, List<Ev>> en : eventos.entrySet()) {
            int no = en.getKey();
            long atual = -1;
            long maior = 0;
            for (Ev e : en.getValue()) {
                Long t = null;
                if (e.nome.equals("ARRANQUE")) {
                    t = Long.parseLong(e.campo("termo"));
                    if (t < atual) { out("[ERRO] n" + no + ": reiniciou com termo " + t + " < " + atual + " (termo não persistido?)"); tudoOk = false; }
                    atual = t;
                } else if (e.nome.equals("TERMO")) {
                    String[] ab = e.det.split("->");
                    long de = Long.parseLong(ab[0].trim()), para = Long.parseLong(ab[1].trim());
                    if (para <= de || (atual >= 0 && de != atual)) { out("[ERRO] n" + no + ": TERMO " + de + "->" + para + " incoerente (estava em " + atual + ")"); tudoOk = false; }
                    atual = para;
                } else if (e.nome.equals("CANDIDATO") || e.nome.equals("LIDER")) {
                    t = Long.parseLong(e.campo("termo"));
                    if (t < atual) { out("[ERRO] n" + no + ": " + e.nome + " no termo " + t + " < " + atual); tudoOk = false; }
                    if (t > atual) atual = t;
                }
                maior = Math.max(maior, atual);
            }
            Path pe = pasta.resolve("estado-p" + k + "-n" + no + ".txt");
            if (Files.isRegularFile(pe)) {
                String l = Files.readString(pe, StandardCharsets.UTF_8).trim();
                long termoGravado = Long.parseLong(l.split(";")[0]);
                if (termoGravado < maior) { out("[ERRO] n" + no + ": estado em disco tem termo " + termoGravado + " mas o nó chegou a " + maior); tudoOk = false; }
            }
        }
        if (tudoOk) out("O termo de cada nó nunca diminui e o estado em disco acompanha-o.");
    }

    // ---------- 3. tempo até ao primeiro líder ----------
    private static void primeiroLider(List<Ev> todos) {
        long arranque = Long.MAX_VALUE, lider = Long.MAX_VALUE;
        for (Ev e : todos) {
            if (e.nome.equals("ARRANQUE")) arranque = Math.min(arranque, e.t);
            if (e.nome.equals("LIDER")) lider = Math.min(lider, e.t);
        }
        out("-- Primeiro líder --");
        if (arranque == Long.MAX_VALUE || lider == Long.MAX_VALUE) out("Sem ARRANQUE ou sem LIDER nos eventos.");
        else out("Primeiro líder " + (lider - arranque) + " ms (" + String.format("%.1f", (lider - arranque) / 1000.0) + " s) depois do arranque do primeiro nó.");
    }

    // ---------- 4. indisponibilidade de cada falha ----------
    private static long[] indisponibilidade(int k, List<String[]> falhas, List<Hb> hbs, List<Ev> todos, List<Leitura> leituras) {
        out("-- Indisponibilidade por falha (falhas.txt) --");
        long fim = hbs.isEmpty() ? 0 : hbs.get(hbs.size() - 1).t;
        for (Ev e : todos) fim = Math.max(fim, e.t);
        boolean alguma = false;
        for (String[] f : falhas) {
            if (Integer.parseInt(f[0].substring(1)) != k) continue;
            alguma = true;
            int no = Integer.parseInt(f[1].substring(1));
            long t = Long.parseLong(f[2]);
            Hb ultimo = null;                                        // último LOTE publicado antes da falha
            for (Hb h : hbs) if (h.t <= t) ultimo = h;
            if (ultimo == null) { out("falha p" + k + ";n" + no + " @" + t + ": ainda não havia líder a publicar."); continue; }
            if (ultimo.no != no) {
                out("falha p" + k + ";n" + no + " @" + t + ": n" + no + " não era o líder (era n" + ultimo.no + ", termo " + ultimo.termo + "): sem indisponibilidade.");
                continue;
            }
            Hb novo = null;
            for (Hb h : hbs) if (h.t > t && h.termo > ultimo.termo) { novo = h; break; }
            long ate = novo != null ? novo.t : fim;
            int enviadas = 0;
            for (Leitura l : leituras) if (l.k == k && l.t >= t && l.t <= ate) enviadas++;
            out("falha p" + k + ";n" + no + " (líder do termo " + ultimo.termo + ") @" + t + ": "
                    + (novo != null ? "indisponibilidade " + (novo.t - t) + " ms (" + String.format("%.1f", (novo.t - t) / 1000.0) + " s), novo líder n" + novo.no + " termo " + novo.termo
                                    : "SEM novo líder até ao fim dos registos (≥ " + (fim - t) + " ms)")
                    + " | leituras enviadas nesse intervalo: " + enviadas);
        }
        if (!alguma) out(falhas.isEmpty() ? "(sem falhas.txt)" : "(sem falhas nesta partição)");
        return new long[0];
    }

    // ---------- 5. TRUNCADO ----------
    private static long[] truncados(int k, List<Ev> todos, Map<Integer, List<Reg>> registos, Path pasta) throws IOException {
        out("-- Registos truncados --");
        long total = 0;
        int ligLider = lider(todos);
        List<Reg> finalLider = registos.getOrDefault(ligLider, List.of());
        for (Ev e : todos) {
            if (!e.nome.equals("TRUNCADO")) continue;
            long desc = Long.parseLong(e.campo("descartados"));
            total += desc;
            // registos descartados por este nó (guardados em truncados-p<k>-n<i>.txt)
            Path pt = pasta.resolve("truncados-p" + k + "-n" + e.no + ".txt");
            String extra = "";
            if (Files.isRegularFile(pt)) {
                List<Reg> desc2 = new ArrayList<>();
                for (String l : Files.readAllLines(pt, StandardCharsets.UTF_8)) {
                    if (l.isBlank()) continue;
                    desc2.add(lerReg(l.substring(l.indexOf(';') + 1)));
                }
                // só os do TRUNCADO actual: seq >= de
                long de = Long.parseLong(e.campo("de"));
                Map<String, Integer> disp = new HashMap<>();
                for (Reg r : finalLider) disp.merge(r.chave(), 1, Integer::sum);
                int existem = 0, contados = 0;
                for (Reg r : desc2) {
                    if (r.seq < de) continue;
                    contados++;
                    Integer c = disp.get(r.chave());
                    for (Reg f : finalLider) {
                        if (f.chave().equals(r.chave()) && f.seq != r.seq && c != null && c > 0) { existem++; disp.put(r.chave(), c - 1); break; }
                    }
                }
                extra = " | destes, existem com outro seq no registo final do líder n" + ligLider + ": " + existem + " de " + contados;
            } else {
                extra = " | (sem truncados-p" + k + "-n" + e.no + ".txt: não sei que registos eram)";
            }
            out("n" + e.no + " @" + e.t + ": TRUNCADO " + e.det + extra);
        }
        if (total == 0) out("Nenhum TRUNCADO.");
        return new long[]{total};
    }

    // líder do maior termo (o último LIDER)
    private static int lider(List<Ev> todos) {
        long maior = -1; int no = -1;
        for (Ev e : todos)
            if (e.nome.equals("LIDER")) { long t = Long.parseLong(e.campo("termo")); if (t >= maior) { maior = t; no = e.no; } }
        return no;
    }
    private static void lidoFinal(int k, List<Ev> todos, Map<Integer, List<Reg>> registos) {
        int l = lider(todos);
        if (l > 0) out("(líder do último termo: n" + l + ")");
    }

    // ---------- 6. coincidência final ----------
    private static long coincidencia(int k, Map<Integer, List<Reg>> registos) {
        out("-- Coincidência final dos ficheiros --");
        if (registos.size() < 2) { out("Menos de 2 ficheiros no-*: nada a comparar."); return 0; }
        int min = Integer.MAX_VALUE;
        StringBuilder tam = new StringBuilder();
        for (Map.Entry<Integer, List<Reg>> e : registos.entrySet()) {
            min = Math.min(min, e.getValue().size());
            tam.append("n").append(e.getKey()).append("=").append(e.getValue().size()).append(" ");
        }
        List<Long> dif = new ArrayList<>();
        for (int s = 0; s < min; s++) {
            String ref = null; boolean difere = false;
            for (List<Reg> l : registos.values()) {
                String x = l.get(s).linha;
                if (ref == null) ref = x; else if (!ref.equals(x)) difere = true;
            }
            if (difere) dif.add((long) s + 1);
        }
        out("registos por nó: " + tam + "| comparados até seq " + min + " (o menor ultimoSeq)");
        out(dif.isEmpty() ? "Os ficheiros coincidem: 0 seq divergentes." : "[DIVERGEM] seq: " + dif);
        return dif.size();
    }

    // ---------- 7. leituras perdidas (globais) ----------
    private static void perdidas(int n, List<Leitura> leituras, List<String[]> falhas, Path pasta) throws IOException {
        out("");
        out("################ Leituras perdidas ################");
        if (leituras.isEmpty()) { out("Sem produtor.txt (ou vazio)."); return; }
        long totalPerdidas = 0;
        for (int k = 0; k < n; k++) {
            boolean temNos = false;
            Map<String, Integer> melhor = new HashMap<>();             // por chave: o máximo que algum nó tem
            for (int i = 1; i <= 3; i++) {
                Path pr = pasta.resolve("no-p" + k + "-n" + i + ".txt");
                if (Files.isRegularFile(pasta.resolve("eventos-p" + k + "-n" + i + ".txt"))) temNos = true;   // o nó correu
                if (!Files.isRegularFile(pr)) continue;
                temNos = true;
                Map<String, Integer> c = new HashMap<>();
                for (Reg r : lerRegistos(pr)) if (!r.sensor.startsWith("ALARM-")) c.merge(r.chave(), 1, Integer::sum);
                for (Map.Entry<String, Integer> e : c.entrySet()) melhor.merge(e.getKey(), e.getValue(), Math::max);
            }
            int enviadas = 0;
            List<Leitura> perdidas = new ArrayList<>();
            for (Leitura l : leituras) {
                if (l.k != k) continue;
                enviadas++;
                if (!temNos) continue;
                Integer c = melhor.get(l.chave());
                if (c != null && c > 0) melhor.put(l.chave(), c - 1); else perdidas.add(l);
            }
            if (enviadas == 0) continue;
            if (!temNos) { out("p" + k + ": " + enviadas + " leituras enviadas, mas a partição não tem ficheiros no-* (nenhum nó correu): não contadas."); continue; }
            out("p" + k + ": enviadas " + enviadas + ", perdidas " + perdidas.size());
            for (Leitura l : perdidas) out("   perdida: " + l.t + ";p" + k + ";" + l.sensor + ";" + l.temp);
            totalPerdidas += perdidas.size();
        }
        out("Total de leituras perdidas: " + totalPerdidas);
    }

    // ---------- monitor-*.txt ----------
    private static void monitores(Path pasta) throws IOException {
        out("");
        out("################ Monitores ################");
        boolean algum = false;
        try (var ds = Files.newDirectoryStream(pasta, "monitor-*.txt")) {
            for (Path p : ds) {
                algum = true;
                Map<String, Integer> c = new TreeMap<>();
                for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                    String[] f = l.split(";", 3);
                    if (f.length >= 2) c.merge(f[1], 1, Integer::sum);
                }
                out(p.getFileName() + ": " + c);
            }
        }
        if (!algum) out("Sem monitor-*.txt.");
    }

    // ---------- leitura de ficheiros ----------
    private static List<Ev> lerEventos(Path p, int no) throws IOException {
        List<Ev> r = new ArrayList<>();
        for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (l.isBlank()) continue;
            String[] f = l.split(";", 4);                 // instante;EVENTO;p<k>;detalhe
            if (f.length < 3) continue;
            r.add(new Ev(Long.parseLong(f[0]), f[1], f.length > 3 ? f[3] : "", no));
        }
        return r;
    }
    private static List<Hb> lerHb(Path p, int no) throws IOException {
        List<Hb> r = new ArrayList<>();
        for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (l.isBlank()) continue;
            String[] f = l.split(";");                    // termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
            r.add(new Hb(Long.parseLong(f[0]), Long.parseLong(f[2]), Long.parseLong(f[3]), no));
        }
        return r;
    }
    private static Reg lerReg(String l) {
        String[] f = l.split(";", 7);                     // seq;termo;sensor;temp;instante;L;V
        Reg r = new Reg();
        r.seq = Long.parseLong(f[0]); r.termo = Long.parseLong(f[1]); r.sensor = f[2];
        r.temp = Double.parseDouble(f[3]); r.linha = l;
        return r;
    }
    private static List<Reg> lerRegistos(Path p) throws IOException {
        List<Reg> r = new ArrayList<>();
        for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) if (!l.isBlank()) r.add(lerReg(l));
        return r;
    }
    private static List<Leitura> lerProdutor(Path p) throws IOException {
        List<Leitura> r = new ArrayList<>();
        if (!Files.isRegularFile(p)) return r;
        for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (l.isBlank()) continue;
            String[] f = l.split(";");                    // instante;p<k>;sensor;temperatura
            Leitura x = new Leitura();
            x.t = Long.parseLong(f[0]); x.k = Integer.parseInt(f[1].substring(1)); x.sensor = f[2]; x.temp = Double.parseDouble(f[3]);
            r.add(x);
        }
        return r;
    }
    private static List<String[]> lerFalhas(Path p) throws IOException {
        List<String[]> r = new ArrayList<>();
        for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (l.isBlank()) continue;
            String[] f = l.trim().split(";");             // p<k>;n<i>;instante
            if (f.length == 3) r.add(f);
        }
        return r;
    }
}