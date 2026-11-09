package raft04;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.BooleanSupplier;

/*
Ensaio <a|b|baleatorio|bfixo|c|d|d1|d2|e|f1|f2|f3|todos>

Corre os ensaios 9(a) a 9(e) da ficha, sozinho. Tem de ser executado DENTRO de raft04-no\ensaio:
  java -Draft04.periodo=2000 -cp ../out raft04.Ensaio a
Precisa de um IPFS (ou do StubIpfs) já a correr. Arranca os nós, o produtor e o processador como processos
filhos (a consola de cada um vai para consola-*.log), provoca as falhas, anota-as em falhas.txt, e no fim
corre o RaftCheck e o CausalCheck. Os relatórios ficam em resultados\<ensaio>-relatorio.txt.

Opções (-D): ensaio.escala (por omissão 1.0) multiplica as esperas longas (30 s, 60 s...) — só para testes;
             ensaio.segunda (3000) ms até à 2.ª rajada durante a eleição (0 desliga);
             ensaio.repsb / ensaio.repsc (10+5 e 3) repetições.
*/
public class Ensaio {

    static final String JAVA = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    static final Path DIR = Paths.get("").toAbsolutePath();
    static final String CP_NO = "../out", CP_PROD = "../../raft04-produtor/out", CP_PROC = "../../raft04-processador/out";
    static final double ESCALA = Double.parseDouble(System.getProperty("ensaio.escala", "1.0"));
    static final long SEGUNDA = Long.getLong("ensaio.segunda", 3000L);

    static final Map<String, Process> procs = new LinkedHashMap<>();
    static PrintWriter prodIn;
    static List<String> extraOpts = new ArrayList<>();

    // ---------------- utilitários ----------------
    static void p(String s) { System.out.println("[ensaio] " + s); }
    static long esc(long ms) { return (long) (ms * ESCALA); }
    static void dorme(long ms) { try { Thread.sleep(ms); } catch (InterruptedException e) { throw new RuntimeException(e); } }
    static boolean espera(BooleanSupplier c, long timeoutMs) {
        long fim = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < fim) { if (c.getAsBoolean()) return true; dorme(50); }
        return c.getAsBoolean();
    }

    static void limpar() throws IOException {
        for (String g : new String[]{"estado-*", "no-*", "eventos-*", "hb-*", "truncados-*", "monitor-*", "consola-*",
                "produtor.txt", "falhas.txt", "vista-lamport.txt", "raftcheck-relatorio.txt", "isolar-*"})
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(DIR, g)) {
                for (Path f : ds) Files.deleteIfExists(f);
            }
    }

    static List<String> jvmOpts() {
        List<String> o = new ArrayList<>();
        o.add(JAVA);
        o.add("-Draft04.periodo=" + Wire.PERIODO_HB_MS);
        for (String k : new String[]{"raft04.eleicaoMin", "raft04.eleicaoMax", "raft04.fator"})
            if (System.getProperty(k) != null) o.add("-D" + k + "=" + System.getProperty(k));
        o.addAll(extraOpts);
        return o;
    }

    static Process arranca(String chave, String cp, String classe, String... args) throws IOException {
        List<String> cmd = jvmOpts();
        cmd.add("-cp"); cmd.add(cp); cmd.add(classe);
        cmd.addAll(Arrays.asList(args));
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(DIR.toFile());
        Path log = DIR.resolve("consola-" + chave + ".log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
        Process pr = pb.start();
        procs.put(chave, pr);
        return pr;
    }
    static void no(int k, int i) throws IOException { arranca("p" + k + "-n" + i, CP_NO, "raft04.PartitionNode2", "" + k, "" + i); }
    static void produtor() throws IOException {
        Process pr = arranca("produtor", CP_PROD, "raft04.Producer", "3");
        prodIn = new PrintWriter(new OutputStreamWriter(pr.getOutputStream(), StandardCharsets.UTF_8), true);
    }
    static void processador() throws IOException { arranca("processador", CP_PROC, "raft04.AlarmProcessor", "3"); }
    static void send(String linha) { prodIn.println(linha); prodIn.flush(); p("produtor <- " + linha); }

    static void mata(int k, int i) {
        Process pr = procs.remove("p" + k + "-n" + i);
        if (pr != null) { pr.destroyForcibly(); try { pr.waitFor(); } catch (InterruptedException e) { } }
    }
    static void marca(int k, int i) throws IOException {
        Files.writeString(DIR.resolve("falhas.txt"), "p" + k + ";n" + i + ";" + System.currentTimeMillis() + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
    static void isola(int k, int i, boolean sim) throws IOException {
        Path f = DIR.resolve("isolar-p" + k + "-n" + i);
        if (sim) Files.writeString(f, "x"); else Files.deleteIfExists(f);
    }

    // eventos de um nó: {instante, EVENTO, p, detalhe}
    static List<String[]> eventos(int k, int i) {
        Path f = DIR.resolve("eventos-p" + k + "-n" + i + ".txt");
        List<String[]> r = new ArrayList<>();
        try {
            if (Files.isRegularFile(f))
                for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    String[] x = l.split(";", 4);
                    if (x.length >= 3) r.add(x);
                }
        } catch (IOException e) { }
        return r;
    }
    static long termoDe(String det) { return Long.parseLong(det.split(";")[0].substring("termo=".length())); }

    // {termo, no} do LIDER de maior termo (ou null)
    static int[] lider(int k) {
        long mt = -1; int mn = -1;
        for (int i = 1; i <= 3; i++)
            for (String[] e : eventos(k, i))
                if (e[1].equals("LIDER") && e.length > 3 && termoDe(e[3]) >= mt) { mt = termoDe(e[3]); mn = i; }
        return mn < 0 ? null : new int[]{(int) mt, mn};
    }
    static int[] esperaLider(int k, long termoMinExcl, long timeoutMs) {
        espera(() -> { int[] l = lider(k); return l != null && l[0] > termoMinExcl; }, timeoutMs);
        int[] l = lider(k);
        return (l != null && l[0] > termoMinExcl) ? l : null;
    }
    static int linhas(String ficheiro) {
        try { Path f = DIR.resolve(ficheiro); return Files.isRegularFile(f) ? (int) Files.readAllLines(f).stream().filter(s -> !s.isBlank()).count() : 0; }
        catch (IOException e) { return 0; }
    }
    static int seq(int k, int i) { return linhas("no-p" + k + "-n" + i + ".txt"); }
    static boolean temEvento(int k, int i, String nome) {
        for (String[] e : eventos(k, i)) if (e[1].equals(nome)) return true;
        return false;
    }
    static long maiorTermo(int k, int i) {
        long m = 0;
        for (String[] e : eventos(k, i)) {
            try {
                if (e[1].equals("TERMO")) m = Math.max(m, Long.parseLong(e[3].split("->")[1].trim()));
                else if ((e[1].equals("ARRANQUE") || e[1].equals("CANDIDATO") || e[1].equals("LIDER")) && e.length > 3) m = Math.max(m, Long.parseLong(e[3].replaceAll(".*(?:^|;)termo=(\\d+).*", "$1")));
            } catch (RuntimeException x) { }
        }
        return m;
    }

    static void fim(String nome) throws IOException {
        for (Process pr : procs.values()) pr.destroyForcibly();
        for (Process pr : procs.values()) { try { pr.waitFor(); } catch (InterruptedException e) { } }
        procs.clear();
        dorme(500);
        boolean falhas = Files.isRegularFile(DIR.resolve("falhas.txt"));
        p("---- RaftCheck " + nome + " ----");
        RaftCheck.main(falhas ? new String[]{"3", ".", "falhas.txt"} : new String[]{"3", "."});
        p("---- CausalCheck " + nome + " ----");
        boolean todas = true;
        for (int k = 0; k < 3; k++) {
            boolean alguma = false;
            for (int i = 1; i <= 3; i++) if (Files.isRegularFile(DIR.resolve("no-p" + k + "-n" + i + ".txt"))) alguma = true;
            if (!alguma) todas = false;
        }
        if (todas) CausalCheck.main(new String[]{"3", "."});
        else p("CausalCheck não aplicável: neste ensaio só correm nós de algumas partições.");
        Files.createDirectories(DIR.resolve("resultados"));
        Files.copy(DIR.resolve("raftcheck-relatorio.txt"), DIR.resolve("resultados").resolve(nome + "-relatorio.txt"), StandardCopyOption.REPLACE_EXISTING);
        p("relatório guardado em resultados\\" + nome + "-relatorio.txt");
    }

    static void arrancaTudo() throws IOException {
        processador();
        produtor();
        for (int k = 0; k < 3; k++) for (int i = 1; i <= 3; i++) no(k, i);
    }
    static boolean todosComLider(long timeoutMs) {
        return espera(() -> lider(0) != null && lider(1) != null && lider(2) != null, timeoutMs);
    }

    // ---------------- (a) Figura 1 ----------------
    static void ensaioA() throws Exception {
        p("=== ENSAIO (a): Figura 1 em p1 ===");
        limpar();
        processador(); produtor();
        for (int i = 1; i <= 3; i++) no(1, i);
        int[] l = esperaLider(1, 0, 90000);
        if (l == null) { p("sem líder: abortado"); fim("a"); return; }
        int L = l[1]; p("líder inicial: n" + L + " (termo " + l[0] + ")");
        send("S02 20.0"); send("S05 21.0");
        final int[] segu = new int[2]; int c = 0;
        for (int i = 1; i <= 3; i++) if (i != L) segu[c++] = i;
        espera(() -> seq(1, segu[0]) >= 2 && seq(1, segu[1]) >= 2, 30000);
        int h0 = linhas("hb-p1-n" + L + ".txt");
        espera(() -> linhas("hb-p1-n" + L + ".txt") > h0, 15000);      // um LOTE acabou de sair
        send("S02 22.0");                                              // 3.ª leitura: só o líder a vai ter
        dorme(250);
        marca(1, L); mata(1, L); p("líder n" + L + " terminado (< 1 s depois da 3.ª leitura)");
        dorme(500); send("S05 23.0");                                  // durante a eleição
        int[] nl = esperaLider(1, l[0], 90000);
        if (nl == null) { p("sem novo líder"); fim("a"); return; }
        p("novo líder: n" + nl[1] + " (termo " + nl[0] + ")");
        espera(() -> linhas("hb-p1-n" + nl[1] + ".txt") >= 1, 15000);
        dorme(300); send("S02 24.0");                                  // depois de haver novo líder
        espera(() -> seq(1, nl[1]) >= 3, 20000);
        no(1, L); p("antigo líder n" + L + " arrancado outra vez");
        boolean trunc = espera(() -> temEvento(1, L, "TRUNCADO"), esc(60000));
        p(trunc ? "TRUNCADO observado." : "ATENÇÃO: não apareceu TRUNCADO (a 3.ª leitura pode ter sido publicada antes de matar o líder: repete).");
        dorme(esc(5000));
        fim("a");
    }

    // ---------------- (b) arranque ----------------
    // aleatorio/fixo: quais dos dois modos correr; ensaio.desde = repetição inicial (para retomar)
    static void ensaioB(boolean aleatorio, boolean fixo) throws Exception {
        p("=== ENSAIO (b): arranque dos nove nós ===");
        Files.createDirectories(DIR.resolve("resultados"));
        int desde = Integer.getInteger("ensaio.desde", 1);
        int repsA = Integer.getInteger("ensaio.repsb", 10), repsF = Math.min(5, repsA);
        for (int modo = 0; modo < 2; modo++) {
            if ((modo == 0 && !aleatorio) || (modo == 1 && !fixo)) continue;
            int reps = modo == 0 ? repsA : repsF;
            extraOpts.clear();
            if (modo == 1) { extraOpts.add("-Draft04.eleicaoMin=6000"); extraOpts.add("-Draft04.eleicaoMax=6000"); }
            String nomeModo = modo == 0 ? "aleatorio" : "fixo";
            for (int r = desde; r <= reps; r++) {
                limpar();
                List<int[]> ordem = new ArrayList<>();
                for (int k = 0; k < 3; k++) for (int i = 1; i <= 3; i++) ordem.add(new int[]{k, i});
                Random rnd = new Random();
                if (modo == 0) Collections.shuffle(ordem, rnd);
                else ordem.sort(Comparator.comparingInt(x -> x[0]));      // os 3 nós de cada partição juntos
                for (int[] x : ordem) { no(x[0], x[1]); if (modo == 0) dorme(rnd.nextInt(800)); }
                boolean ok = todosComLider(esc(modo == 0 ? 60000 : 90000));
                dorme(500);
                StringBuilder sb = new StringBuilder("b-" + nomeModo + " #" + r + ": ");
                for (int k = 0; k < 3; k++) sb.append(resume(k)).append(" | ");
                if (!ok) sb.append("[TIMEOUT sem líder em alguma partição]");
                p(sb.toString());
                Files.writeString(DIR.resolve("resultados").resolve("b-resumo.txt"), sb + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                fim("b-" + nomeModo + "-" + r);
            }
        }
        extraOpts.clear();
        p("resumo de (b) em resultados\\b-resumo.txt");
    }
    // "p<k>: primeiro líder X ms, termo T, N termos, V votos divididos"
    static String resume(int k) {
        long t0 = Long.MAX_VALUE, t1 = Long.MAX_VALUE, termoLider = -1;
        Map<Long, Set<Integer>> cand = new TreeMap<>(); Set<Long> comLider = new HashSet<>();
        for (int i = 1; i <= 3; i++)
            for (String[] e : eventos(k, i)) {
                long t = Long.parseLong(e[0]);
                if (e[1].equals("ARRANQUE")) t0 = Math.min(t0, t);
                if (e[1].equals("CANDIDATO")) cand.computeIfAbsent(termoDe(e[3]), x -> new HashSet<>()).add(i);
                if (e[1].equals("LIDER")) { comLider.add(termoDe(e[3])); if (t < t1) { t1 = t; termoLider = termoDe(e[3]); } }
            }
        int divididos = 0;
        for (Map.Entry<Long, Set<Integer>> en : cand.entrySet()) if (en.getValue().size() >= 2 && !comLider.contains(en.getKey())) divididos++;
        if (t1 == Long.MAX_VALUE) return "p" + k + ": sem líder (" + cand.size() + " termos, " + divididos + " votos divididos)";
        return "p" + k + ": líder em " + (t1 - t0) + " ms, termo " + termoLider + ", " + cand.size() + " termos, " + divididos + " votos divididos";
    }

    // ---------------- (c) falha do líder durante rajada ----------------
    static void ensaioC() throws Exception {
        p("=== ENSAIO (c): falha do líder de p1 durante uma rajada ===");
        int reps = Integer.getInteger("ensaio.repsc", 3);
        for (int r = Integer.getInteger("ensaio.desde", 1); r <= reps; r++) {
            limpar(); arrancaTudo();
            if (!todosComLider(esc(120000))) { p("sem líderes: repetição abortada"); fim("c-" + r); continue; }
            dorme(esc(2000));
            int[] l = lider(1); int L = l[1];
            send("rajada 300 10"); dorme(300);
            marca(1, L); mata(1, L); p("líder de p1 (n" + L + ") terminado");
            if (SEGUNDA > 0) { dorme(SEGUNDA); send("rajada 300 10"); }       // durante a eleição
            int[] nl = esperaLider(1, l[0], 90000);
            if (nl != null) { dorme(2000); send("rajada 300 10"); }            // já com o novo líder
            dorme(Math.max(0, esc(30000) - SEGUNDA - 2000));
            no(1, L); p("n" + L + " arrancado 30 s depois");
            dorme(esc(25000));
            fim("c-" + r);
        }
    }

    // ---------------- (d) isolamento ----------------
    static void ensaioD(boolean d1, boolean d2) throws Exception {
        //int[] l;
        if (d1) ensaioD1();
        if (d2) ensaioD2();
    }
    static void ensaioD1() throws Exception {
        p("=== ENSAIO (d1): isolar o líder de p1 durante 30 s, com rajada ===");
        limpar(); arrancaTudo();
        if (!todosComLider(esc(120000))) { p("sem líderes"); fim("d1"); return; }
        dorme(esc(2000));
        int[] l = lider(1); int L = l[1];
        send("rajada 300 10"); dorme(300);
        marca(1, L); isola(1, L, true); p("líder n" + L + " isolado");
        dorme(esc(15000)); send("rajada 300 10");                       // já há novo líder: os dois recebem
        dorme(esc(15000));
        marca(1, L); isola(1, L, false); p("partição reparada");
        dorme(esc(25000));
        fim("d1");
    }
    static void ensaioD2() throws Exception {
        int[] l;
        p("=== ENSAIO (d2): isolar um seguidor de p1 durante 60 s, sem rajada ===");
        limpar();
        processador(); produtor();
        for (int i = 1; i <= 3; i++) no(1, i);
        l = esperaLider(1, 0, 90000);
        if (l == null) { p("sem líder"); fim("d2"); return; }
        int F = l[1] == 1 ? 2 : 1;
        int[] ll = l;
        dorme(esc(3000));
        marca(1, F); isola(1, F, true); p("seguidor n" + F + " isolado (o líder é n" + ll[1] + ", termo " + ll[0] + ")");
        dorme(esc(60000));
        long tF = maiorTermo(1, F);
        p("termo atingido pelo seguidor isolado: " + tF);
        marca(1, F); isola(1, F, false); p("seguidor n" + F + " reparado");
        dorme(esc(25000));
        int[] depois = lider(1);
        p("líder no fim: n" + depois[1] + " termo " + depois[0] + " (antes: n" + ll[1] + " termo " + ll[0] + ")");
        fim("d2");
    }

    // ---------------- (e) dois nós em baixo ----------------
    static void ensaioE() throws Exception {
        p("=== ENSAIO (e): terminar dois dos três nós de p1 ===");
        limpar();
        processador(); produtor();
        for (int i = 1; i <= 3; i++) no(1, i);
        int[] l = esperaLider(1, 0, 90000);
        if (l == null) { p("sem líder"); fim("e"); return; }
        int S = l[1] == 1 ? 2 : 1;                                      // sobrevivente: um seguidor
        List<Integer> mortos = new ArrayList<>();
        for (int i = 1; i <= 3; i++) if (i != S) mortos.add(i);
        dorme(esc(3000));
        for (int m : mortos) { marca(1, m); mata(1, m); }
        p("nós " + mortos + " terminados; sobrevivente n" + S);
        long fimLeituras = System.currentTimeMillis() + esc(30000);
        int c = 0;
        while (System.currentTimeMillis() < fimLeituras) { send((c++ % 2 == 0 ? "S02 " : "S05 ") + (20 + c % 5) + ".0"); dorme(3000); }
        long termoS = maiorTermo(1, S);
        long candidaturas = eventos(1, S).stream().filter(e -> e[1].equals("CANDIDATO")).count();
        p("sobrevivente n" + S + ": termo " + termoS + ", " + candidaturas + " candidaturas");
        long ant = lider(1)[0];
        int R = mortos.get(0);
        long t0 = System.currentTimeMillis();
        no(1, R); p("n" + R + " arrancado outra vez");
        int[] nl = esperaLider(1, ant, 120000);
        p(nl == null ? "sem líder após o regresso" : "líder n" + nl[1] + " (termo " + nl[0] + ") " + (System.currentTimeMillis() - t0) + " ms depois do regresso");
        dorme(esc(5000));
        fim("e");
    }


    // ---------------- falhas deliberadas (f1..f3) ----------------
    // f1: maioria errada; arranque com prazo fixo para forçar candidaturas simultâneas
    static void ensaioF1() throws Exception {
        p("=== f1: arranque dos nove nós com prazo fixo (para a falha da maioria) ===");
        extraOpts.clear(); extraOpts.add("-Draft04.eleicaoMin=6000"); extraOpts.add("-Draft04.eleicaoMax=6000");
        for (int r = 1; r <= 3; r++) {
            limpar();
            for (int k = 0; k < 3; k++) for (int i = 1; i <= 3; i++) no(k, i);
            todosComLider(esc(60000)); dorme(3000);
            fim("f1-" + r);
        }
        extraOpts.clear();
    }
    // f2: termo/votedFor só em memória; um seguidor é reiniciado logo depois de haver líder
    static void ensaioF2() throws Exception {
        p("=== f2: reiniciar um seguidor logo após a eleição ===");
        for (int r = 1; r <= 3; r++) {
            limpar();
            for (int i = 1; i <= 3; i++) no(1, i);
            int[] l = esperaLider(1, 0, 90000);
            if (l == null) { fim("f2-" + r); continue; }
            int F = l[1] == 1 ? 2 : 1;
            mata(1, F); no(1, F);
            dorme(esc(8000));
            fim("f2-" + r);
        }
    }
    // f3: voto sem upToDate; um seguidor atrasado (isolado) pode ganhar a eleição seguinte
    static void ensaioF3() throws Exception {
        p("=== f3: seguidor atrasado a candidatar-se (falha do upToDate) ===");
        int tent = Integer.getInteger("ensaio.tentativas", 3);
        for (int t = 1; t <= tent; t++) {
            limpar();
            processador(); produtor();
            for (int i = 1; i <= 3; i++) no(1, i);
            int[] l = esperaLider(1, 0, 90000);
            if (l == null) { fim("f3-" + t); continue; }
            int L = l[1], B = L == 1 ? 2 : 1, C = 6 - L - B;
            isola(1, B, true);
            send("S02 20.0"); send("S05 21.0"); send("S02 22.0");
            espera(() -> seq(1, C) >= 3, 20000);
            marca(1, L); mata(1, L);
            isola(1, B, false);
            p("tentativa " + t + ": líder n" + L + " morto; n" + B + " atrasado (candidato possível), n" + C + " actualizado");
            esperaLider(1, l[0], 90000);
            dorme(esc(10000));
            boolean manifestou = temEvento(1, C, "TRUNCADO");
            p(manifestou ? "A FALHA MANIFESTOU-SE: o nó actualizado n" + C + " teve de truncar registos." : "nesta tentativa o atrasado não ganhou.");
            fim("f3-" + t);
            if (manifestou) break;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) { System.out.println("Uso: java raft04.Ensaio <a|b|baleatorio|bfixo|c|d|d1|d2|e|f1|f2|f3|todos>"); return; }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { for (Process pr : procs.values()) pr.destroyForcibly(); }));
        try {
            switch (args[0]) {
                case "a": ensaioA(); break;
                case "b": ensaioB(true, true); break;
                case "baleatorio": ensaioB(true, false); break;
                case "bfixo": ensaioB(false, true); break;
                case "c": ensaioC(); break;
                case "d": ensaioD(true, true); break;
                case "d1": ensaioD(true, false); break;
                case "d2": ensaioD(false, true); break;
                case "f1": ensaioF1(); break;
                case "f2": ensaioF2(); break;
                case "f3": ensaioF3(); break;
                case "e": ensaioE(); break;
                case "todos": ensaioA(); ensaioB(true, true); ensaioC(); ensaioD(true, true); ensaioE(); break;
                default: System.out.println("Ensaio desconhecido: " + args[0]);
            }
        } finally {
            for (Process pr : procs.values()) pr.destroyForcibly();
        }
        System.exit(0);
    }
}