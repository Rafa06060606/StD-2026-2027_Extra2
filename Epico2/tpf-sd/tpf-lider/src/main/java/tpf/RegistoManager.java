package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.*;

/**
 * Gere a numeração LSN, o log durável e o chunk atualmente aberto.
 * O método registar é synchronized para que duas mensagens simultâneas não recebam o mesmo LSN.
 */
public final class RegistoManager implements AutoCloseable {
    private final Path logPath;
    private final Path eventosPath;
    private final Sealed sealed;
    private final ScheduledExecutorService scheduler;
    private long ultimoLsn;
    private Chunk chunkAberto;
    private ScheduledFuture<?> tarefaSelagem;

    public RegistoManager(Path pastaDados, byte[] masterKey) throws Exception {
        Files.createDirectories(pastaDados);
        this.logPath = pastaDados.resolve("registos.log");
        this.eventosPath = pastaDados.resolve("eventos-lider.txt");
        this.sealed = new Sealed(pastaDados.resolve("selados"), masterKey);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "selagem-chunks");
            t.setDaemon(true);
            return t;
        });
        this.ultimoLsn = recuperarUltimoLsn();
    }

    /**
     * Único caminho de escrita para /registar, mensagens do canal e /rajada.
     * Só devolve o registo depois de FileChannel.force(true) terminar com sucesso.
     */
    public synchronized Registo registar(String autor, String canal, String texto) throws Exception {
        if (autor == null || canal == null || texto == null) throw new IllegalArgumentException("Autor, canal e texto são obrigatórios.");
        long lsn = ultimoLsn + 1;
        Registo registo = new Registo(lsn, Instant.now().toEpochMilli(), autor, canal, texto);
        // LSN e instante permanecem numéricos; os três campos textuais são codificados em Base64 UTF-8.
        byte[] linha = (registo.lsn() + ";" + registo.instante() + ";"
                + codificar(registo.autor()) + ";"
                + codificar(registo.canal()) + ";"
                + codificar(registo.texto()) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);

        // Abrimos em APPEND e forçamos os dados para armazenamento antes de confirmar o registo.
        try (FileChannel channel = FileChannel.open(logPath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer buffer = ByteBuffer.wrap(linha);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }

        // Só avançamos o LSN em memória depois da persistência durável ter sido bem-sucedida.
        ultimoLsn = lsn;
        try {
            escreverEvento("REGISTO", "LSN=" + lsn);
        } catch (IOException e) {
            // O log principal já está durável; uma falha no ficheiro de eventos não deve perder o registo.
            System.err.println("Aviso: não foi possível escrever o evento REGISTO: " + e.getMessage());
        }
        adicionarAoChunk(registo);
        return registo;
    }

    private static String codificar(String valor) {
        return Base64.getEncoder().encodeToString(valor.getBytes(StandardCharsets.UTF_8));
    }

    private long recuperarUltimoLsn() throws IOException {
        if (!Files.exists(logPath)) return 0;
        long ultimo = 0;
        int numeroLinha = 0;
        for (String linha : Files.readAllLines(logPath, StandardCharsets.UTF_8)) {
            numeroLinha++;
            if (linha.isBlank()) continue;
            String[] campos = linha.split(";", -1);
            if (campos.length != 5) throw new IOException("Linha " + numeroLinha + " inválida em registos.log.");
            try {
                long lsn = Long.parseLong(campos[0]);
                // O instante é numérico; validamos os restantes campos Base64 para detetar corrupção.
                Long.parseLong(campos[1]);
                decodificar(campos[2]); decodificar(campos[3]); decodificar(campos[4]);
                if (lsn != ultimo + 1) throw new IOException("Sequência LSN inválida na linha " + numeroLinha + ".");
                ultimo = lsn;
            } catch (IllegalArgumentException e) {
                throw new IOException("Conteúdo inválido na linha " + numeroLinha + " de registos.log.", e);
            }
        }
        return ultimo;
    }

    private static String decodificar(String valor) {
        return new String(Base64.getDecoder().decode(valor), StandardCharsets.UTF_8);
    }

    private void adicionarAoChunk(Registo registo) throws Exception {
        if (chunkAberto == null) {
            chunkAberto = new Chunk(System.currentTimeMillis());
            // O prazo é contado a partir do primeiro registo do chunk e não é reiniciado pelos seguintes.
            tarefaSelagem = scheduler.schedule(() -> {
                synchronized (RegistoManager.this) {
                    try { selarChunkAberto(); }
                    catch (Exception e) { System.err.println("Erro na selagem temporizada: " + e.getMessage()); }
                }
            }, Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }
        chunkAberto.adicionar(registo);

        // O registo que faz ultrapassar o limite já foi acrescentado; é incluído neste chunk selado.
        int tamanhoAtual = chunkAberto.serializar().length;
        if (tamanhoAtual >= Wire.CHUNK_MAX_BYTES) selarChunkAberto();
    }

    private void selarChunkAberto() throws Exception {
        if (chunkAberto == null || chunkAberto.estaVazio()) return;
        if (tarefaSelagem != null) tarefaSelagem.cancel(false);

        Chunk aSelar = chunkAberto;
        // Mantemos a referência até a gravação cifrada terminar com sucesso.
        byte[] serializado = aSelar.serializar();
        byte[] comPadding = Sealed.prepararConteudo(serializado);
        String id = sealed.guardar(comPadding);
        long tamanho = comPadding.length;
        chunkAberto = null;
        tarefaSelagem = null;
        try {
            escreverEvento("SELADO", "chunkId=" + id + ";lsnInicio=" + aSelar.primeiroLsn()
                    + ";lsnFim=" + aSelar.ultimoLsn() + ";tamanho=" + tamanho);
        } catch (IOException e) {
            // Os ficheiros do chunk já estão gravados; não repetimos a cifra por falha no log de eventos.
            System.err.println("Aviso: chunk selado, mas não foi possível escrever o evento SELADO: " + e.getMessage());
        }
        System.out.println("Chunk selado: " + id + " (LSN " + aSelar.primeiroLsn() + "-" + aSelar.ultimoLsn() + ")");
    }

    private synchronized void escreverEvento(String evento, String detalhe) throws IOException {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe + System.lineSeparator();
        Files.writeString(eventosPath, linha, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public synchronized long getUltimoLsn() { return ultimoLsn; }

    /** Fecha o agendador e tenta selar o chunk pendente antes de terminar normalmente. */
    @Override
    public synchronized void close() throws Exception {
        scheduler.shutdown();
        if (tarefaSelagem != null) tarefaSelagem.cancel(false);
        selarChunkAberto();
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) scheduler.shutdownNow();
    }

    /** Método de apoio para testes: permite abrir um chunk selado e validar os registos. */
    public List<Registo> abrirChunk(String chunkId) throws Exception { return sealed.abrir(chunkId); }
}
