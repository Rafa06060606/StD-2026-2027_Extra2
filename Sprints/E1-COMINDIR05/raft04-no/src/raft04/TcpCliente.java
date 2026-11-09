package raft04;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

// Ferramenta de teste: java raft04.TcpCliente <porto> <de> <ate>
public class TcpCliente {
    public static void main(String[] args) throws Exception {
        int porto = Integer.parseInt(args[0]);
        try (Socket s = new Socket("127.0.0.1", porto);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
            s.setSoTimeout(3000);
            out.println("PEDIDO " + args[1] + " " + args[2]);
            String line;
            while ((line = in.readLine()) != null) System.out.println("< " + line);
            System.out.println("(ligação fechada pelo servidor)");
        }
    }
}