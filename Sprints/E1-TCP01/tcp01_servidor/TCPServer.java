package tcp01;

import java.io.*;
import java.net.*;

public class TCPServer {
    
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        try {
            int serverPort = 7896;
            ServerSocket listenSocket = new ServerSocket(serverPort);
            System.out.println("Servidor TCP à escuta no porto " + serverPort + "...");
            
            while (true) {
                Socket clientSocket = listenSocket.accept(); // Bloqueia à espera de ligação[cite: 1]
                new Connection(clientSocket);               // Instancia e inicia a thread diretamente[cite: 1]
            }
        } catch (IOException e) {
            System.out.println("Listen: " + e.getMessage());
        }
    }
}