package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.Role;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;
import java.nio.file.Path;
import java.io.IOException;

import java.util.Set;

public class LeaderBot extends ListenerAdapter {

    private final RegistoManager registoManager;

    public LeaderBot(RegistoManager registoManager) {
        this.registoManager = registoManager;
    }

    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String PAPEL_ADMIN = "admin-arquivo";
    private static final Set<String> COMANDOS_ADMIN = Set.of(
            "config", "replica-token", "replica-remover", "rajada"
    );

    public static void main(String[] args) throws InterruptedException {

        // 1. Ler o token e o ID do servidor
        String token = System.getProperty("tpf.token", System.getenv("DISCORD_TOKEN"));
        String guildId = System.getProperty("tpf.guild", System.getenv("DISCORD_GUILD_ID"));

        if (token == null || token.isBlank()) {
            System.err.println("ERRO: Token do Discord não definido.");
            System.exit(1);
            return;
        }

        if (guildId == null || guildId.isBlank()) {
            System.err.println("ERRO: ID do servidor não definido.");
            System.exit(1);
            return;
        }

        try {
            // 2. Validar os parâmetros -Dtpf.* e inicializar o armazenamento
            Wire.validarConfiguracao();
            Path pastaDados = Path.of("dados");

            byte[] masterKey = MasterKey.carregarOuCriar(
                    pastaDados.resolve("master.key")
            );

            RegistoManager registoManager = new RegistoManager(
                    pastaDados,
                    masterKey
            );

            System.out.println("Armazenamento do líder inicializado com sucesso.");

            // 3. Criar o bot com acesso ao RegistoManager
            LeaderBot leaderBot = new LeaderBot(registoManager);

            // 4. Iniciar a ligação ao Discord
            JDA jda = JDABuilder.createDefault(token)
                    .enableIntents(
                            GatewayIntent.GUILD_MESSAGES,
                            GatewayIntent.MESSAGE_CONTENT
                    )
                    .addEventListeners(leaderBot)
                    .build()
                    .awaitReady();

            // 5. Obter o servidor Discord
            Guild guild = jda.getGuildById(guildId);

            if (guild == null) {
                System.err.println(
                        "ERRO: O bot não encontrou o servidor com ID " + guildId
                );
                jda.shutdown();
                System.exit(1);
                return;
            }

            // 6. Registar os comandos Slash
            guild.updateCommands().addCommands(

                    Commands.slash(
                            "registar",
                            "Regista uma entrada no arquivo distribuído"
                    ).addOption(
                            OptionType.STRING,
                            "texto",
                            "Texto a registar",
                            true
                    ),

                    Commands.slash(
                            "estado",
                            "Mostra o estado das réplicas e chunks subreplicados"
                    ),

                    Commands.slash(
                            "config",
                            "Altera parâmetros de configuração do líder (Admin)"
                    ).addSubcommands(
                            new SubcommandData(
                                    "r",
                                    "Altera o fator de replicação R"
                            ).addOption(
                                    OptionType.STRING,
                                    "n",
                                    "Novo fator de replicação",
                                    true
                            )
                    ),

                    Commands.slash(
                            "recuperar",
                            "Recupera os registos num intervalo de LSN"
                    ).addOption(
                            OptionType.STRING,
                            "de",
                            "LSN inicial",
                            true
                    ).addOption(
                            OptionType.STRING,
                            "ate",
                            "LSN final",
                            true
                    ),

                    Commands.slash(
                            "replica-token",
                            "Gera um token de uso único para registar uma nova réplica (Admin)"
                    ),

                    Commands.slash(
                            "replica-remover",
                            "Remove uma réplica registada (Admin)"
                    ).addOption(
                            OptionType.STRING,
                            "id",
                            "Identificador da réplica",
                            true
                    ),

                    Commands.slash(
                            "rajada",
                            "Injeta n registos sintéticos para ensaios (Admin)"
                    ).addOption(
                            OptionType.STRING,
                            "n",
                            "Número de registos a injetar",
                            true
                    ).addOption(
                            OptionType.STRING,
                            "intervaloms",
                            "Intervalo em milissegundos",
                            true
                    )

            ).queue(
                    sucesso -> System.out.println(
                            "LeaderBot online! " + sucesso.size()
                                    + " comandos registados no servidor "
                                    + guild.getName()
                    ),
                    erro -> System.err.println(
                            "Falha ao registar comandos: " + erro.getMessage()
                    )
            );

        } catch (Exception e) {
            System.err.println("Erro ao iniciar o líder: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String cmd = event.getName();

        // 1. Verificar permissões para comandos restritos a admin-arquivo
        if (COMANDOS_ADMIN.contains(cmd)) {
            if (!temPapelAdmin(event.getMember())) {
                event.reply("Acesso recusado: o comando `/" + cmd + "` requer o papel `" + PAPEL_ADMIN + "`.").setEphemeral(true).queue();
                return;
            }
        }

        // 2. Identificar comando, ler e validar argumentos
        switch (cmd) {
            
            case "registar" -> {
                String texto = getStringArg(event, "texto");

                if (texto.isBlank()) {
                    event.reply("Erro: o texto não pode estar vazio.")
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                try {
                    // Obter os dados de quem executou o comando
                    String autor = event.getUser().getName();
                    String canal = event.getChannel().getName();

                    // Registar através do método central do RegistoManager
                    Registo registo = registoManager.registar(
                            autor,
                            canal,
                            texto
                    );

                    // Só responder depois de o registo estar persistido
                    event.reply(
                            "Registo guardado com sucesso!\n" +
                            "LSN atribuído: **" + registo.lsn() + "**"
                    ).queue();

                } catch (Exception e) {
                    System.err.println("Erro ao registar mensagem: " + e.getMessage());
                    e.printStackTrace();

                    event.reply("Não foi possível guardar o registo. Tenta novamente.")
                            .setEphemeral(true)
                            .queue();
                }
            }


            case "estado" -> {
                event.reply("`/estado` recebido — ainda não implementado").queue();
            }

            case "config" -> {
                String sub = event.getSubcommandName();
                if (!"r".equals(sub)) {
                    event.reply("Erro: subcomando de `/config` desconhecido.").setEphemeral(true).queue();
                    return;
                }
                String nStr = getStringArg(event, "n");
                Integer n = parseIntSafe(nStr);
                if (n == null || n < 1) {
                    event.reply("Erro em `/config r`: o argumento `n` (\"" + nStr + "\") é inválido. Deve ser um número inteiro >= 1.").setEphemeral(true).queue();
                    return;
                }
                event.reply("`/config r` recebido: n=" + n + " — ainda não implementado").queue();
            }

            case "recuperar" -> {
                String deStr = getStringArg(event, "de");
                String ateStr = getStringArg(event, "ate");
                Long de = parseLongSafe(deStr);
                Long ate = parseLongSafe(ateStr);

                if (de == null || ate == null) {
                    event.reply("Erro em `/recuperar`: `de` (\"" + deStr + "\") e `ate` (\"" + ateStr + "\") têm de ser números inteiros.").setEphemeral(true).queue();
                    return;
                }
                if (de < 1 || ate < de) {
                    event.reply("Erro em `/recuperar`: intervalo inválido (de=" + de + ", ate=" + ate + "). Deve cumprir 1 <= de <= ate.").setEphemeral(true).queue();
                    return;
                }
                event.reply("`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
            }

            case "replica-token" -> {
                event.reply("`/replica-token` recebido — ainda não implementado").queue();
            }

            case "replica-remover" -> {
                String id = getStringArg(event, "id");
                if (id.isBlank() || id.contains(";")) {
                    event.reply("Erro em `/replica-remover`: o `id` da réplica é inválido.").setEphemeral(true).queue();
                    return;
                }
                event.reply("`/replica-remover` recebido: id=" + id + " — ainda não implementado").queue();
            }

            case "rajada" -> {
                String nStr = getStringArg(event, "n");
                String intStr = getStringArg(event, "intervaloms");
                Integer n = parseIntSafe(nStr);
                Long intervaloMs = parseLongSafe(intStr);

                if (n == null || intervaloMs == null || n < 1 || intervaloMs < 0) {
                    event.reply("Erro em `/rajada`: argumentos inválidos (n=\"" + nStr + "\", intervaloMs=\"" + intStr + "\"). `n` deve ser >= 1 e `intervaloMs` >= 0.").setEphemeral(true).queue();
                    return;
                }
                // A rajada pode demorar (cada registo faz fsync). Confirmamos já a interação ao Discord
                // e executamos o trabalho noutra thread para não bloquear a thread de eventos do JDA.
                event.deferReply().queue();
                final int total = n;
                final long espera = intervaloMs;
                final String autor = event.getUser().getName();
                final String canal = event.getChannel().getName();

                Thread rajada = new Thread(() -> executarRajada(event, total, espera, autor, canal), "rajada");
                rajada.setDaemon(true);
                rajada.start();
            }

            default -> event.reply("Comando desconhecido.").setEphemeral(true).queue();
        }
    }

    
    /** Tamanho do texto sintético: suficiente para que 500 registos excedam vários chunks de 64 KB. */
    private static final int TAMANHO_TEXTO_RAJADA = 400;

    /**
     * Injeta n registos sintéticos pelo MESMO método usado por /registar (RegistoManager.registar),
     * para testar o caminho real de escrita, e não um caminho alternativo.
     */
    private void executarRajada(SlashCommandInteractionEvent event, int n, long intervaloMs,
                                String autor, String canal) {
        int registados = 0;
        long primeiroLsn = -1;
        long ultimoLsn = -1;
        try {
            for (int i = 1; i <= n; i++) {
                Registo r = registoManager.registar(autor, canal, textoSintetico(i, n));
                if (primeiroLsn < 0) primeiroLsn = r.lsn();
                ultimoLsn = r.lsn();
                registados++;
                if (intervaloMs > 0 && i < n) Thread.sleep(intervaloMs);
            }
            event.getHook().sendMessage(
                    "Rajada concluída: **" + registados + "** registos (LSN " + primeiroLsn + " a " + ultimoLsn + ")."
            ).queue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            event.getHook().sendMessage("Rajada interrompida após " + registados + " registos.").queue();
        } catch (Exception e) {
            System.err.println("Erro na rajada: " + e.getMessage());
            e.printStackTrace();
            event.getHook().sendMessage(
                    "A rajada falhou após " + registados + " de " + n + " registos. Os registos já confirmados mantêm-se guardados."
            ).queue();
        }
    }

    /** Gera um texto previsível de TAMANHO_TEXTO_RAJADA caracteres, útil para reconhecer registos nos testes. */
    private static String textoSintetico(int i, int n) {
        StringBuilder sb = new StringBuilder("RAJADA_" + i + "_DE_" + n + "_");
        while (sb.length() < TAMANHO_TEXTO_RAJADA) sb.append('X');
        return sb.toString();
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {

        // Ignorar mensagens de bots
        if (event.getAuthor().isBot()) {
            return;
        }

        // Ignorar mensagens fora do canal #arquivo
        if (!event.getChannel().getName().equals(CANAL_ARQUIVO)) {
            return;
        }

        // Obter os dados da mensagem
        String autor = event.getAuthor().getName();
        String canal = event.getChannel().getName();
        String texto = event.getMessage().getContentRaw();

        // Ignorar mensagens vazias
        if (texto.isBlank()) {
            return;
        }

        try {
            Registo registo = registoManager.registar(autor, canal, texto);

            System.out.println(
                "Mensagem registada com sucesso. LSN: " + registo.lsn()
            );

        } catch (Exception e) {
            System.err.println("Erro ao registar mensagem do Discord: "
                    + e.getMessage());
            e.printStackTrace();
        }
    }


    private boolean temPapelAdmin(Member member) {
        if (member == null) return false;
        for (Role role : member.getRoles()) {
            if (PAPEL_ADMIN.equals(role.getName())) {
                return true;
            }
        }
        return false;
    }

    private String getStringArg(SlashCommandInteractionEvent event, String name) {
        OptionMapping opt = event.getOption(name);
        return opt != null ? opt.getAsString().trim() : "";
    }

    private Integer parseIntSafe(String val) {
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Long parseLongSafe(String val) {
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}