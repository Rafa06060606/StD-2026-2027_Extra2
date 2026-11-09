import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * IPFS FALSO, só para desenvolver e testar sem o IPFS instalado.
 * Imita três chamadas da API do Kubo: /version, /pubsub/pub e /pubsub/sub.
 * Tudo o que alguém publica num tópico é entregue a todos os que o subscreveram
 * (incluindo o próprio publicador, como faz o IPFS real).
 *
 * Correr:  java StubIpfs.java        (porto 5001; outro porto:  java StubIpfs.java 5002)
 */
public class StubIpfs {

    static final Map<String, List<OutputStream>> subscritores = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        int porto = args.length > 0 ? Integer.parseInt(args[0]) : 5001;
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", porto), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        server.createContext("/api/v0/version", ex -> {
            byte[] r = "{\"Version\":\"stub\"}".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, r.length);
            ex.getResponseBody().write(r);
            ex.close();
        });

        // subscrever: a ligação fica aberta e recebe uma linha JSON por mensagem
        server.createContext("/api/v0/pubsub/sub", ex -> {
            String topico = argumento(ex);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, 0);
            OutputStream os = ex.getResponseBody();
            subscritores.computeIfAbsent(topico, t -> new CopyOnWriteArrayList<>()).add(os);
            System.out.println("[stub] nova subscrição em " + nome(topico));
            os.write("{}\n".getBytes(StandardCharsets.UTF_8));
            os.flush();
            try { new CountDownLatch(1).await(); } catch (InterruptedException e) { }
        });

        // publicar: lê o corpo (multipart) e entrega a mensagem a todos os subscritores do tópico
        server.createContext("/api/v0/pubsub/pub", ex -> {
            String topico = argumento(ex);
            String corpo = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String ct = ex.getRequestHeaders().getFirst("Content-Type");
            String fronteira = ct.substring(ct.indexOf("boundary=") + 9);
            int ini = corpo.indexOf("\r\n\r\n") + 4;
            int fim = corpo.lastIndexOf("\r\n--" + fronteira);
            String msg = corpo.substring(ini, fim);

            String mb = "u" + Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(msg.getBytes(StandardCharsets.UTF_8));
            byte[] linha = ("{\"from\":\"stub\",\"data\":\"" + mb + "\",\"seqno\":\"x\",\"topicIDs\":[\"t\"]}\n")
                    .getBytes(StandardCharsets.UTF_8);

            for (OutputStream os : subscritores.getOrDefault(topico, List.of())) {
                try {
                    synchronized (os) { os.write(linha); os.flush(); }
                } catch (IOException e) {
                    subscritores.get(topico).remove(os);   // subscritor desligou-se
                }
            }
            String primeira = msg.split("\\R", 2)[0];
            System.out.println("[stub] " + nome(topico) + " <- " + (primeira.length() > 90 ? primeira.substring(0, 90) + "..." : primeira));
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });

        server.start();
        System.out.println("IPFS FALSO à escuta em http://127.0.0.1:" + porto + "/api/v0  (Ctrl+C para parar)");
    }

    static String argumento(HttpExchange ex) {
        String q = ex.getRequestURI().getRawQuery();
        return q.substring(q.indexOf("arg=") + 4);
    }

    static String nome(String multibase) {
        try {
            return new String(Base64.getUrlDecoder().decode(multibase.substring(1)), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return multibase;
        }
    }
}