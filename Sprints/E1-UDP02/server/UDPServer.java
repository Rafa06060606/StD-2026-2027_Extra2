import java.net.*;
import java.io.*;
import java.util.*;

public class UDPServer {

    // Lista de mensagens efetivamente entregues por ordem (Lista de Receção)
    private static List<String> recebidasEmOrdem = new ArrayList<>();
    
    // Estrutura temporária para guardar mensagens adiantadas (chave = N, valor = Texto)
    private static Map<Integer, String> temporaria = new HashMap<>();

    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        int L = 0; // Estado L: número da última mensagem entregue em ordem (inicia a 0)

        try {
            aSocket = new DatagramSocket(6789); // Liga o servidor ao porto fixo 6789
            byte[] buffer = new byte[1000];

            System.out.println("Servidor UDP à escuta no porto 6789...");

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request); // Aguarda datagrama do cliente

                // Converte os dados recebidos para String
                String dadosRecebidos = new String(request.getData(), 0, request.getLength()).trim();

                String respostaStr;

                try {
                    // Tenta fazer o parsing da mensagem no formato "<N>, <mensagem>"
                    int posVirgula = dadosRecebidos.indexOf(',');
                    if (posVirgula == -1) {
                        throw new IllegalArgumentException("Formato inválido. Falta a vírgula.");
                    }

                    int N = Integer.parseInt(dadosRecebidos.substring(0, posVirgula).trim());
                    String texto = dadosRecebidos.substring(posVirgula + 1).trim();

                    // Se a mensagem for válida, verifica se já foi entregue ou se é nova
                    if (N <= L) {
                        // Mensagem duplicada/antiga que já tinha sido entregue
                        respostaStr = "waitingfor, " + (L + 1);
                    } else if (N == L + 1) {
                        // Mensagem esperada! Processa a entrega em cascata
                        L = processDeliveredMessages(L, N, texto);
                        respostaStr = "echo, " + texto;
                    } else {
                        // Mensagem adiantada (N > L + 1): Guarda na estrutura temporária
                        temporaria.put(N, texto);
                        respostaStr = "waitingfor, " + (L + 1);
                    }

                } catch (Exception e) {
                    // Captura erros de formatação mantendo o servidor a funcionar sem ir abaixo
                    respostaStr = "erro, mensagem mal formada";
                }

                // Imprime na consola do servidor o estado após o processamento deste datagrama
                System.out.println("----------------------------------------");
                System.out.println("L após processamento: " + L);
                System.out.println("Estrutura temporária: " + temporaria);
                System.out.println("Lista de receção (em ordem): " + recebidasEmOrdem);

                // Envia a resposta ao cliente
                byte[] respBytes = respostaStr.getBytes();
                DatagramPacket reply = new DatagramPacket(
                    respBytes, respBytes.length, request.getAddress(), request.getPort()
                );
                aSocket.send(reply);
            }

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }

    /**
     * Entrega em cascata: regista a mensagem atual e verifica se as mensagens
     * seguintes já se encontravam guardadas na estrutura temporária.
     */
    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        int L = nLastMessageInOrder;

        // 1. Entregar a mensagem corrente que acabou de chegar
        recebidasEmOrdem.add(currentMessage);
        L = nCurrentMessage; // Atualiza L

        // 2. Cascata: enquanto o próximo número (L + 1) existir na estrutura temporária...
        while (temporaria.containsKey(L + 1)) {
            int proximoN = L + 1;
            String textoGuardado = temporaria.remove(proximoN); // Remove da temporária e obtém o texto
            recebidasEmOrdem.add(textoGuardado); // Adiciona à lista de entregues em ordem
            L = proximoN; // Incrementa L
        }

        return L; // Retorna o novo valor final de L
    }
}