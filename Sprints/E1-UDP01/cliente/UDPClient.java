import java.net.*; // Importa as classes de rede do Java (DatagramSocket, DatagramPacket, InetAddress, etc.)[cite: 1]
import java.io.*;  // Importa as classes para manipulação de fluxos de entrada/saída (BufferedReader, InputStreamReader, IOException)[cite: 1]

public class UDPClient { // Declaração da classe pública do cliente UDP[cite: 1]

  public static void main(String args[]) { // Ponto de entrada principal do programa Java[cite: 1]
    DatagramSocket aSocket = null; // Declara a variável do socket UDP, inicializando-a a null para permitir o seu encerramento posterior no bloco 'finally'[cite: 1]

    try { // Inicia o bloco 'try' para capturar e tratar potenciais exceções de rede/IO[cite: 1]
      aSocket = new DatagramSocket(); // Cria o socket UDP do cliente num porto efémero (dinâmico e livre) atribuído automaticamente pelo Sistema Operativo[cite: 1]
      aSocket.setSoTimeout(5000); // Define um tempo limite de espera (5000ms = 5s) para o método receive(); evita que o cliente fique bloqueado para sempre se a resposta se perder[cite: 1]

      InetAddress aHost = InetAddress.getByName("localhost"); // Resolve o nome de domínio "localhost" para o endereço IP da própria máquina (127.0.0.1)[cite: 1]
      int serverPort = 6789; // Define o porto fixo onde o servidor UDP está à escuta[cite: 1]

      BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in)); // Cria um leitor para recolher o texto introduzido pelo utilizador na consola/teclado[cite: 1]
      String line; // Declara a variável do tipo String para armazenar cada linha digitada pelo utilizador[cite: 1]
      byte[] buffer = new byte[1000]; // Aloca um array de 1000 bytes em memória para armazenar os dados do datagrama de resposta do servidor[cite: 1]

      System.out.println("Type messages to send. Enter 'quit' to exit."); // Imprime uma mensagem na consola a instruir o utilizador sobre como usar e sair do programa[cite: 1]
      while ((line = stdin.readLine()) != null) { // Ciclo de repetição que lê continuamente o texto do teclado linha a linha até atingir o fim da entrada[cite: 1]
        if (line.equalsIgnoreCase("quit")) break; // Se o utilizador escrever "quit" (ignorando maiúsculas/minúsculas), interrompe o ciclo e encerra o cliente[cite: 1]

        byte[] m = line.getBytes(); // Converte o texto lido (String) num array de bytes, formato exigido pelos datagramas UDP[cite: 1]
        DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort); // Encapsula os dados num datagrama com a mensagem, tamanho, endereço IP e porto de destino[cite: 1]
        aSocket.send(request); // Envia efetivamente o pacote de dados (datagrama) através do socket para o servidor[cite: 1]

        DatagramPacket reply = new DatagramPacket(buffer, buffer.length); // Cria um datagrama vazio associado ao buffer de 1000 bytes para receber a resposta[cite: 1]
        try { // Bloco 'try' interno para tratar individualmente o tempo limite de espera (timeout) de cada receção[cite: 1]
          aSocket.receive(reply); // Bloqueia a execução do cliente à espera de receber um pacote de resposta vindo do servidor[cite: 1]
          System.out.println("Reply: " + new String(reply.getData(), 0, reply.getLength())); // Converte para String apenas os bytes úteis recebidos (do índice 0 até ao comprimento real) e imprime no ecrã[cite: 1]
        } catch (java.net.SocketTimeoutException e) { // Captura a exceção disparada caso os 5 segundos de espera passem sem chegar nenhuma resposta[cite: 1]
          System.out.println("No response (timeout)"); // Informa o utilizador na consola de que o tempo de espera esgotou sem resposta[cite: 1]
        }
      }

    } catch (SocketException e) { System.out.println("Socket: " + e.getMessage()); // Trata erros associados à criação ou configuração do socket UDP[cite: 1]
    } catch (IOException e)     { System.out.println("IO: " + e.getMessage());     // Trata erros de entrada/saída (por exemplo, falhas de leitura do teclado)[cite: 1]
    } finally { if (aSocket != null) aSocket.close(); } // Executa sempre ao terminar: garante que o socket de rede é devidamente fechado libertando os recursos[cite: 1]
  }
}