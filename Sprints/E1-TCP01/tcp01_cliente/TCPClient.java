package tcp01; // Corrigido para tcp01

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;
            s = new Socket("localhost", serverPort);

            // Criar ObjectOutputStream primeiro para evitar deadlock de streams
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());

            // Instanciar o Place e depois a Person com o Place incluído
            Place place = new Place("3500-000", "Viseu");
            Person p = new Person("Ana Maria", place, 1998);

            out.writeObject(p);
            out.flush();

            String response = in.readUTF();
            System.out.println("Resposta do servidor: " + response);

        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}