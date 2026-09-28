import java.net.*; // Importa as classes de rede do Java (DatagramSocket, DatagramPacket, etc.)[cite: 1]
import java.io.*;  // Importa as classes de entrada/saída do Java[cite: 1]

public class UDPServer { // Declaração da classe pública do servidor UDP[cite: 1]

  public static void main(String args[]) { // Ponto de entrada principal do programa Java[cite: 1]
    DatagramSocket aSocket = null; // Declara a variável do socket UDP, inicializando-a a null para permitir o seu encerramento posterior[cite: 1]

    try { // Inicia o bloco 'try' para capturar e tratar potenciais exceções de rede/IO[cite: 1]
      aSocket = new DatagramSocket(6789); // Cria o socket do servidor associando-o (bind) ao porto fixo e conhecido 6789[cite: 1]
      byte[] buffer = new byte[1000]; // Aloca um array de 1000 bytes em memória para servir de buffer de receção das mensagens dos clientes[cite: 1]

      while (true) { // Ciclo infinito para manter o servidor continuamente ativo e a atender pedidos[cite: 1]
        DatagramPacket request = new DatagramPacket(buffer, buffer.length); // Prepara um datagrama vazio associado ao buffer para acolher os dados do próximo pedido[cite: 1]
        aSocket.receive(request); // Bloqueia a execução e aguarda até que chegue um datagrama enviado por um cliente[cite: 1]

        DatagramPacket reply = new DatagramPacket(request.getData(), // Cria o datagrama de resposta (echo) usando exatamente os mesmos dados recebidos[cite: 1]
            request.getLength(), request.getAddress(), request.getPort()); // Define o tamanho útil e extrai o IP e porto de origem do cliente diretamente do pedido para saber para onde devolver a resposta[cite: 1]

        aSocket.send(reply); // Envia o datagrama de resposta de volta ao cliente[cite: 1]
      }
    } catch (SocketException e) { System.out.println("Socket: " + e.getMessage()); // Trata erros associados ao socket (como a tentativa de usar um porto já ocupado)[cite: 1]
    } catch (IOException e)     { System.out.println("IO: " + e.getMessage());     // Trata erros genéricos de transmissão/receção na rede[cite: 1]
    } finally { if (aSocket != null) aSocket.close(); } // Garante o fecho do socket de rede caso o ciclo infinito seja interrompido por um erro grave[cite: 1]
  }
}