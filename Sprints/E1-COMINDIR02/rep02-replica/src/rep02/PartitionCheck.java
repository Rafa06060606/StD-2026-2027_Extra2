package rep02;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionCheck {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java rep02.PartitionCheck <k> [N]");
            return;
        }

        int k, n;
        try {
            k = Integer.parseInt(args[0]);
            n = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        } catch (NumberFormatException e) {
            System.out.println("Erro: <k> e [N] devem ser números inteiros.");
            return;
        }

        if (n <= 0 || k < 0 || k >= n) {
            System.out.println("Erro: Partição inválida (k=" + k + " para N=" + n + ").");
            return;
        }

        Partitioner partitioner = new Partitioner(n);
        String leaderFileName = "lider-p" + k + ".txt";
        File leaderFile = new File(leaderFileName);

        System.out.println("=== Validação da Partição p" + k + " (N=" + n + ") ===");

        // 1. Encontrar lider-p<k>.txt
        if (!leaderFile.exists()) {
            System.out.println("[ERRO] Ficheiro do líder não encontrado: " + leaderFileName);
            return;
        }

        List<SensorRecord> leaderRecords;
        try {
            RecordFile leaderRecordFile = new RecordFile(leaderFileName);
            leaderRecords = leaderRecordFile.range(1, Long.MAX_VALUE);
            System.out.println("1. Líder encontrado: " + leaderFileName + " (" + leaderRecords.size() + " registos)");
        } catch (Exception e) {
            System.out.println("[ERRO] Falha ao ler o ficheiro do líder: " + e.getMessage());
            return;
        }

        // 2. Encontrar todos os ficheiros replica-*-p<k>.txt
        File currentDir = new File(".");
        File[] replicaFiles = currentDir.listFiles((dir, name) -> name.matches("replica-.*-p" + k + "\\.txt"));
        if (replicaFiles == null) {
            replicaFiles = new File[0];
        }
        Arrays.sort(replicaFiles, Comparator.comparing(File::getName));
        System.out.println("2. Réplicas encontradas: " + replicaFiles.length);

        // 3. Validar a sequência do líder e 4. Verificar se cada sensor pertence a k
        long expectedSeq = 1;
        boolean leaderSeqValid = true;
        boolean sensorsValid = true;
        Set<String> distinctSensors = new HashSet<>();

        for (SensorRecord r : leaderRecords) {
            if (r.getSeq() != expectedSeq) {
                System.out.println("   [ERRO] Quebra de sequência no líder! Esperado: " + expectedSeq + ", Encontrado: " + r.getSeq());
                leaderSeqValid = false;
            }
            expectedSeq = r.getSeq() + 1;

            String sensor = r.getSensor();
            distinctSensors.add(sensor);

            int expectedPartition = partitioner.of(sensor);
            if (expectedPartition != k) {
                System.out.println("   [ERRO] Sensor '" + sensor + "' (seq " + r.getSeq() + ") pertence à partição p" + expectedPartition + ", mas está em p" + k);
                sensorsValid = false;
            }
        }

        if (leaderSeqValid) {
            System.out.println("3. Sequência do líder: OK (1.." + leaderRecords.size() + ")");
        }
        if (sensorsValid) {
            System.out.println("4. Particionamento de sensores: OK (" + distinctSensors.size() + " sensores distintos)");
        }

        // 5. Comparar cada réplica com o líder
        int syncedReplicas = 0;
        for (File repFile : replicaFiles) {
            try {
                RecordFile repRecordFile = new RecordFile(repFile.getName());
                List<SensorRecord> repRecords = repRecordFile.range(1, Long.MAX_VALUE);

                boolean sameSize = (repRecords.size() == leaderRecords.size());
                boolean sameContent = true;
                int limit = Math.min(repRecords.size(), leaderRecords.size());

                for (int i = 0; i < limit; i++) {
                    if (!repRecords.get(i).toLine().equals(leaderRecords.get(i).toLine())) {
                        sameContent = false;
                        System.out.println("   [ERRO] Divergência em " + repFile.getName() + " na posição " + (i + 1) +
                                " (esperado: " + leaderRecords.get(i).toLine() + ", encontrado: " + repRecords.get(i).toLine() + ")");
                        break;
                    }
                }

                if (sameSize && sameContent) {
                    syncedReplicas++;
                    System.out.println("5. Réplica " + repFile.getName() + ": OK (" + repRecords.size() + " registos)");
                } else if (!sameSize && sameContent) {
                    System.out.println("5. Réplica " + repFile.getName() + ": INCOMPLETA/DIFERENTE (" + repRecords.size() + "/" + leaderRecords.size() + " registos)");
                } else {
                    System.out.println("5. Réplica " + repFile.getName() + ": DIVERGENTE (" + repRecords.size() + " registos)");
                }
            } catch (Exception e) {
                System.out.println("5. Erro ao ler réplica " + repFile.getName() + ": " + e.getMessage());
            }
        }

        // 6. Contar registos, sensores distintos e réplicas
        System.out.println("\n--- Resumo Final (p" + k + ") ---");
        System.out.println("Registos no líder: " + leaderRecords.size());
        System.out.println("Sensores distintos: " + distinctSensors.size());
        System.out.println("Réplicas sincronizadas: " + syncedReplicas + "/" + replicaFiles.length);
    }
}