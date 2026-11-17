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

import java.util.Set;

public class LeaderBot extends ListenerAdapter {

    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String PAPEL_ADMIN = "admin-arquivo";
    private static final Set<String> COMANDOS_ADMIN = Set.of(
            "config", "replica-token", "replica-remover", "rajada"
    );

    public static void main(String[] args) throws InterruptedException {
        // Lê o token e o Guild ID de variáveis de ambiente ou propriedades de sistema (-Dtpf.token=...)
        String token = System.getProperty("tpf.token", System.getenv("DISCORD_TOKEN"));
        String guildId = System.getProperty("tpf.guild", System.getenv("DISCORD_GUILD_ID"));

        if (token == null || token.isBlank()) {
            System.err.println("ERRO: Token do Discord não definido. Define a variável de ambiente DISCORD_TOKEN ou -Dtpf.token=...");
            System.exit(1);
        }
        if (guildId == null || guildId.isBlank()) {
            System.err.println("ERRO: ID do servidor (Guild ID) não definido. Define DISCORD_GUILD_ID ou -Dtpf.guild=...");
            System.exit(1);
        }

        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot())
                .build()
                .awaitReady();

        Guild guild = jda.getGuildById(guildId);
        if (guild == null) {
            System.err.println("ERRO: O bot não encontrou o servidor com ID " + guildId + ". Já adicionaste o bot ao servidor?");
            System.exit(1);
        }

        // Registar os 7 slash commands diretamente no servidor de teste (atualização imediata)
        guild.updateCommands().addCommands(
                Commands.slash("registar", "Regista uma entrada no arquivo distribuído")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                Commands.slash("estado", "Mostra o estado das réplicas e chunks subreplicados"),

                Commands.slash("config", "Altera parâmetros de configuração do líder (Admin)")
                        .addSubcommands(
                                new SubcommandData("r", "Altera o fator de replicação R")
                                        .addOption(OptionType.STRING, "n", "Novo fator de replicação (inteiro >= 1)", true)
                        ),

                Commands.slash("recuperar", "Recupera os registos num intervalo de LSN")
                        .addOption(OptionType.STRING, "de", "LSN inicial (>= 1)", true)
                        .addOption(OptionType.STRING, "ate", "LSN final (>= de)", true),

                Commands.slash("replica-token", "Gera um token de uso único para registar uma nova réplica (Admin)"),

                Commands.slash("replica-remover", "Remove uma réplica registada (Admin)")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica (ex.: A)", true),

                Commands.slash("rajada", "Injeta n registos sintéticos para ensaios (Admin)")
                        .addOption(OptionType.STRING, "n", "Número de registos a injetar (> 0)", true)
                        .addOption(OptionType.STRING, "intervaloms", "Intervalo em milissegundos entre registos (>= 0)", true)
        ).queue(
                sucesso -> System.out.println("LeaderBot online! " + sucesso.size() + " comandos registados no servidor " + guild.getName()),
                erro -> System.err.println("Falha ao registar comandos: " + erro.getMessage())
        );
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
                    event.reply("Erro: o argumento `texto` não pode estar vazio.").setEphemeral(true).queue();
                    return;
                }
                event.reply("`/registar` recebido: texto=\"" + texto + "\" — ainda não implementado").queue();
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
                event.reply("`/rajada` recebido: n=" + n + ", intervaloMs=" + intervaloMs + " — ainda não implementado").queue();
            }

            default -> event.reply("Comando desconhecido.").setEphemeral(true).queue();
        }
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        // Ignorar mensagens enviadas por bots (incluindo o próprio LeaderBot)
        if (event.getAuthor().isBot()) {
            return;
        }

        // Ignorar mensagens que não sejam no canal #arquivo
        if (!CANAL_ARQUIVO.equals(event.getChannel().getName())) {
            return;
        }

        String autor = event.getAuthor().getName();
        String conteudo = event.getMessage().getContentRaw();

        System.out.println("[#arquivo] Mensagem detetada de " + autor + ": " + conteudo);
        event.getChannel().sendMessage(
                "Mensagem em `#arquivo` recebida de **" + autor + "**: \"" + conteudo + "\" — registo ainda não implementado"
        ).queue();
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