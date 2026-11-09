package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

// Anota uma falha em falhas.txt (formato p<k>;n<i>;instante), para o RaftCheck.
// Uso: java raft04.MarcarFalha <k> <i> [ficheiro]     (por omissão falhas.txt na pasta actual)
public class MarcarFalha {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) { System.out.println("Uso: java raft04.MarcarFalha <k> <i> [falhas.txt]"); return; }
        String f = args.length > 2 ? args[2] : "falhas.txt";
        String linha = "p" + Integer.parseInt(args[0]) + ";n" + Integer.parseInt(args[1]) + ";" + System.currentTimeMillis()
                + System.lineSeparator();
        Files.writeString(Paths.get(f), linha, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.print("Anotado em " + f + ": " + linha);
    }
}