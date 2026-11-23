package rep02;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionCheck {

    public static void main(String[] args) {
        int n = 3;
        File dir = new File(".");

        if (args.length >= 1) {
            try {
                n = Integer.parseInt(args[0]);
                if (n <= 0) {
                    System.out.println("Erro: O número de partições <N> deve ser > 0.");
                    return;
                }
            } catch (NumberFormatException e) {
                System.out.println("Uso: java rep02.PartitionCheck <N> [pasta]");
                return;
            }
        }
        if (args.length >= 2) {
            dir = new File(args[1]);
        }

        if (!dir.exists() || !dir.isDirectory()) {
            System.out.println("Erro: Pasta não encontrada: " + dir.getPath());
            return;
        }

        Partitioner partitioner = new Partitioner(n);
        int totalSystemRecords = 0;
        Set<String> allSystemSensors = new HashSet<>();
        boolean allOk = true;

        System.out.println("=== Verificação do Sistema (N = " + n + ", pasta = " + dir.getPath() + ") ===");

        for (int k = 0; k < n; k++) {
            final int partK = k;
            System.out.println("\n--- Partição p" + k + " ---");

            File leaderFile = new File(dir, "lider-p" + k + ".txt");
            if (!leaderFile.exists()) {
                System.out.println("[ERRO] Ficheiro do líder não encontrado: " + leaderFile.getPath());
                allOk = false;
                continue;
            }

            List<SensorRecord> leaderRecords;
            try {
                RecordFile rf = new RecordFile(leaderFile.getPath());
                leaderRecords = rf.range(1, Long.MAX_VALUE);
            } catch (Exception e) {
                System.out.println("[ERRO] Falha ao ler " + leaderFile.getName() + ": " + e.getMessage());
                allOk = false;
                continue;
            }

            // Validar sequência (1..M sem lacunas nem repetições) e chave de partição of(sensor) == k
            long expectedSeq = 1;
            boolean seqOk = true;
            boolean partOk = true;
            Set<String> partSensors = new HashSet<>();

            for (SensorRecord r : leaderRecords) {
                if (r.getSeq() != expectedSeq) {
                    System.out.println("  [ERRO] Quebra de sequência em " + leaderFile.getName() + ": esperado " + expectedSeq + ", encontrado " + r.getSeq());
                    seqOk = false;
                    allOk = false;
                }
                expectedSeq = r.getSeq() + 1;

                String sensor = r.getSensorId();
                partSensors.add(sensor);
                allSystemSensors.add(sensor);

                int calcPart = partitioner.of(sensor);
                if (calcPart != k) {
                    System.out.println("  [ERRO] Sensor '" + sensor + "' (seq " + r.getSeq() + ") pertence a p" + calcPart + ", mas está em p" + k);
                    partOk = false;
                    allOk = false;
                }
            }

            totalSystemRecords += leaderRecords.size();

            System.out.println("Líder (" + leaderFile.getName() + "): " + leaderRecords.size() + " registos | "
                    + partSensors.size() + " sensores distintos | sequência: " + (seqOk ? "OK" : "ERRO")
                    + " | partição: " + (partOk ? "OK" : "ERRO"));

            // Procurar réplicas replica-*-p<k>.txt
            File[] replicaFiles = dir.listFiles((d, name) -> name.matches("replica-.*-p" + partK + "\\.txt"));
            if (replicaFiles == null) {
                replicaFiles = new File[0];
            }
            Arrays.sort(replicaFiles, Comparator.comparing(File::getName));

            System.out.println("Réplicas encontradas para p" + k + ": " + replicaFiles.length);
            for (File repFile : replicaFiles) {
                try {
                    RecordFile repRf = new RecordFile(repFile.getPath());
                    List<SensorRecord> repRecords = repRf.range(1, Long.MAX_VALUE);

                    boolean isPrefix = repRecords.size() <= leaderRecords.size();
                    for (int i = 0; i < Math.min(repRecords.size(), leaderRecords.size()); i++) {
                        if (!repRecords.get(i).toLine().equals(leaderRecords.get(i).toLine())) {
                            isPrefix = false;
                            break;
                        }
                    }

                    int missing = leaderRecords.size() - repRecords.size();
                    if (isPrefix && missing == 0) {
                        System.out.println("  - " + repFile.getName() + ": IGUAL AO LÍDER (" + repRecords.size() + " registos, 0 em falta)");
                    } else if (isPrefix && missing > 0) {
                        System.out.println("  - " + repFile.getName() + ": PREFIXO DO LÍDER (" + repRecords.size() + "/" + leaderRecords.size() + " registos, " + missing + " em falta)");
                    } else {
                        System.out.println("  - " + repFile.getName() + ": DIVERGENTE (" + repRecords.size() + " registos)");
                        allOk = false;
                    }
                } catch (Exception e) {
                    System.out.println("  - [ERRO] Falha ao ler " + repFile.getName() + ": " + e.getMessage());
                    allOk = false;
                }
            }
        }

        System.out.println("\n=== Resumo Global do Sistema ===");
        System.out.println("Total de registos (soma das " + n + " partições): " + totalSystemRecords);
        System.out.println("Total de sensores distintos no sistema: " + allSystemSensors.size());
        System.out.println("Estado global: " + (allOk ? "VÁLIDO (OK)" : "COM ERROS/DIVERGÊNCIAS"));
    }
}