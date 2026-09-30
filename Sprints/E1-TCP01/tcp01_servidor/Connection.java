package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream in;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            in = new ObjectInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start();
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            Object obj = in.readObject();
            if (obj instanceof Person) {
                Person p = (Person) obj;
                // Alterado de p.getName() para p.getPlace().getLocality() conforme a etapa 4.3
                out.writeUTF("Localidade recebida: " + p.getPlace().getLocality());
            } else {
                out.writeUTF("Objeto de tipo desconhecido.");
            }
        } catch (ClassNotFoundException e) {
            System.out.println("Classe não encontrada: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                /* falha ao fechar */
            }
        }
    }
}