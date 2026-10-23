
package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CausalCheck {

    // Representa um registo e a partição onde foi escrito.
    private static class Evento {
        int k;
        SensorRecord registo;

        Evento(int k, SensorRecord registo) {
            this.k = k;
            this.registo = registo;
        }
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println(
                "Uso: CausalCheck <N> <pasta>"
            );
            return;
        }

        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);

        if (n < 1) {
            System.out.println("N tem de ser maior que zero.");
            return;
        }

        try {
            analisar(n, pasta);
        } catch (IOException e) {
            System.err.println(
                "Erro ao ler ou escrever ficheiros: "
                + e.getMessage()
            );
        } catch (RuntimeException e) {
            System.err.println(
                "Erro nos dados: " + e.getMessage()
            );
        }
    }

    private static void analisar(int n, Path pasta)
            throws IOException {

        List<Evento> eventos = new ArrayList<>();
        boolean ficheirosComProblemas = false;

        System.out.println("=== CAUSAL CHECK ===");
        System.out.println("Número de partições: " + n);
        System.out.println("Pasta: " + pasta.toAbsolutePath());

        // 1. Ler os ficheiros dos líderes.
        for (int k = 0; k < n; k++) {

            Path ficheiro = pasta.resolve(
                "lider-p" + k + ".txt"
            );

            if (!Files.isRegularFile(ficheiro)) {
                System.out.println(
                    "[ERRO] Ficheiro em falta: " + ficheiro
                );
                ficheirosComProblemas = true;
                continue;
            }

            List<String> linhas = Files.readAllLines(
                ficheiro, StandardCharsets.UTF_8
            );

            int registosLidos = 0;

            for (int numeroLinha = 1;
                 numeroLinha <= linhas.size();
                 numeroLinha++) {

                String linha = linhas.get(numeroLinha - 1);

                if (linha.isBlank()) {
                    continue;
                }

                try {
                    SensorRecord r =
                        SensorRecord.fromLine(linha);

                    eventos.add(new Evento(k, r));
                    registosLidos++;

                    // Verificar V[k] == seq.
                    long componente = r.getVector().get(k);

                    if (componente != r.getSeq()) {
                        System.out.println(
                            "[ERRO V[k] != seq] p" + k
                            + ", linha " + numeroLinha
                            + ": seq=" + r.getSeq()
                            + ", V[" + k + "]=" + componente
                        );
                        ficheirosComProblemas = true;
                    }

                } catch (RuntimeException e) {
                    System.out.println(
                        "[ERRO] Registo inválido em "
                        + ficheiro + ", linha "
                        + numeroLinha + ": " + linha
                    );
                    ficheirosComProblemas = true;
                }
            }

            System.out.println(
                "p" + k + ": " + registosLidos
                + " registos lidos."
            );
        }

        if (ficheirosComProblemas) {
            System.out.println(
                "AVISO: existem ficheiros ou registos "
                + "com problemas. Confirma o relatório."
            );
        }

        // 2. Comparar todos os pares de partições diferentes.
        long aAntesB = 0;
        long bAntesA = 0;
        long concorrentes = 0;
        long vetoresIguais = 0;
        long violacoesLamport = 0;
        long concorrentesLamportDistinto = 0;

        for (int i = 0; i < eventos.size(); i++) {

            Evento a = eventos.get(i);

            for (int j = i + 1; j < eventos.size(); j++) {

                Evento b = eventos.get(j);

                // A ficha pede pares de partições diferentes.
                if (a.k == b.k) {
                    continue;
                }

                VectorClock.Order ordem =
                    a.registo.getVector().compare(
                        b.registo.getVector()
                    );

                if (ordem == VectorClock.Order.BEFORE) {

                    aAntesB++;

                    if (a.registo.getLamport()
                            >= b.registo.getLamport()) {
                        violacoesLamport++;
                        System.out.println(
                            "[VIOLAÇÃO LAMPORT] "
                            + descricao(a) + " -> "
                            + descricao(b)
                        );
                    }

                } else if (ordem == VectorClock.Order.AFTER) {

                    bAntesA++;

                    if (b.registo.getLamport()
                            >= a.registo.getLamport()) {
                        violacoesLamport++;
                        System.out.println(
                            "[VIOLAÇÃO LAMPORT] "
                            + descricao(b) + " -> "
                            + descricao(a)
                        );
                    }

                } else if (ordem == VectorClock.Order.CONCURRENT) {

                    concorrentes++;

                    if (a.registo.getLamport()
                            != b.registo.getLamport()) {
                        concorrentesLamportDistinto++;
                    }

                } else {
                    // Vetores iguais: não são BEFORE nem AFTER,
                    // mas também não satisfazem a definição
                    // estrita de concorrência.
                    vetoresIguais++;
                }
            }
        }

        // 3. Mostrar os resultados.
        long causais = aAntesB + bAntesA;
        long totalComparado =
            causais + concorrentes + vetoresIguais;

        double proporcao = concorrentes == 0
            ? 0.0
            : 100.0 * concorrentesLamportDistinto
                / concorrentes;

        System.out.println();
        System.out.println("=== RESULTADOS ===");
        System.out.println("Registos analisados: " + eventos.size());
        System.out.println("Pares causais a -> b: " + aAntesB);
        System.out.println("Pares causais b -> a: " + bAntesA);
        System.out.println("Total de pares causais: " + causais);
        System.out.println("Pares concorrentes: " + concorrentes);
        System.out.println("Pares com vetores iguais: " + vetoresIguais);
        System.out.println("Pares comparados: " + totalComparado);
        System.out.println(
            "Violações da condição de Lamport: "
            + violacoesLamport
        );
        System.out.println(
            "Pares concorrentes com L distinto: "
            + concorrentesLamportDistinto
        );
        System.out.printf(
            "Proporção de concorrentes com L distinto: %.2f%%%n",
            proporcao
        );

        // 4. Criar vista-lamport.txt, ordenada por (L, k).
        eventos.sort(
            Comparator
                .comparingLong(
                    (Evento e) -> e.registo.getLamport()
                )
                .thenComparingInt(e -> e.k)
                .thenComparingLong(e -> e.registo.getSeq())
        );

        Path vista = pasta.resolve("vista-lamport.txt");

        List<String> linhasVista = new ArrayList<>();

        for (Evento e : eventos) {
            // Identificamos a partição e mantemos a linha original.
            linhasVista.add(
                "p" + e.k + ";" + e.registo.toLine()
            );
        }

        Files.write(
            vista,
            linhasVista,
            StandardCharsets.UTF_8
        );

        System.out.println();
        System.out.println(
            "Vista de Lamport escrita em: "
            + vista.toAbsolutePath()
        );
        System.out.println("=== FIM DA ANÁLISE ===");
    }

    private static String descricao(Evento e) {
        return "p" + e.k + "#" + e.registo.getSeq()
            + " L=" + e.registo.getLamport()
            + " V=" + e.registo.getVector();
    }
}
