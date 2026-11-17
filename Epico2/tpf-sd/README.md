# TPF Sistemas Distribuídos — Arquivo Distribuído Zero-Knowledge

Este projeto implementa o **líder** (`tpf-lider`) de um arquivo distribuído. Este README cobre o
**Sprint E2-01** (bot Discord e comandos) e o **Sprint E2-02** (registo durável, chunks e cifra).

Réplicas, catálogo e recuperação ficam para sprints seguintes.

---

## 1. Pré-requisitos

- Java 17 ou superior
- Maven 3.8+
- Aplicação e servidor Discord configurados (canal `#arquivo`, papel `admin-arquivo`)

## 2. Como iniciar o líder

1. Definir o token do bot e o ID do servidor (**nunca** guardar no repositório):

   Linux / macOS:
   ```bash
   export DISCORD_TOKEN="o_teu_token_aqui"
   export DISCORD_GUILD_ID="id_do_servidor_aqui"
   ```
   Windows (PowerShell):
   ```powershell
   $env:DISCORD_TOKEN="o_teu_token_aqui"
   $env:DISCORD_GUILD_ID="id_do_servidor_aqui"
   ```

2. Compilar e arrancar a partir da pasta `tpf-lider/`:
   ```bash
   mvn clean compile
   mvn exec:java
   ```

3. Correr os testes:
   ```bash
   mvn test
   ```

## 3. Pasta `dados/`

A pasta é criada no diretório de execução e **não é enviada para o Git** (ver `.gitignore`).

| Ficheiro | Conteúdo |
|---|---|
| `registos.log` | Registos duráveis, uma linha por registo |
| `master.key` | Chave mestra de 32 bytes (segredo, só do líder) |
| `eventos-lider.txt` | Eventos `REGISTO` e `SELADO` (`instante;evento;detalhe`) |
| `selados/` | Chunks cifrados: `<chunkId>.bin` e `<chunkId>.hdr` |

## 4. Chave mestra (`master.key`)

- Tem exatamente **32 bytes** (256 bits).
- No primeiro arranque, se não existir, é gerada com `SecureRandom`, e as permissões de leitura/escrita
  ficam restritas ao dono (em sistemas POSIX; em Windows mantêm-se as ACLs do sistema).
- Nos arranques seguintes é carregada. **Nunca** é gerada uma chave nova se já existir uma, senão os
  chunks anteriores deixavam de poder ser abertos.
- Se existir mas não tiver 32 bytes, o **arranque é recusado** com uma exceção.
- Nunca é enviada para o Discord, para o Git ou para outros componentes.

## 5. Parâmetros `-Dtpf.*`

Os valores por omissão podem ser alterados no arranque, sem mexer no código:

| Propriedade | Valor por omissão | Significado |
|---|---|---|
| `tpf.chunk.maxBytes` | `65536` | Tamanho máximo do chunk (conteúdo serializado) antes de ser selado |
| `tpf.pad.bytes` | `4096` | O conteúdo é completado até um múltiplo deste valor |
| `tpf.selagem.ms` | `5000` | Tempo máximo, desde o primeiro registo, até selar o chunk aberto |

Exemplo:
```bash
mvn exec:java -Dtpf.selagem.ms=10000 -Dtpf.chunk.maxBytes=8192
```

Os valores têm de ser positivos; uma configuração inválida impede o arranque
(`Wire.validarConfiguracao()`).

## 6. Fluxo de um registo

1. `/registar <texto>` ou uma mensagem em `#arquivo` chama **o mesmo método**,
   `RegistoManager.registar(autor, canal, texto)`, que é `synchronized`.
2. É atribuído o LSN seguinte e a linha é acrescentada a `registos.log`.
3. É feito `FileChannel.force(true)` (fsync). **Só depois** é que o líder responde com o LSN.
4. É escrito o evento `REGISTO` e o registo entra no chunk aberto.
5. O chunk é selado quando atinge `CHUNK_MAX_BYTES` (o registo que ultrapassa o limite fica nesse chunk)
   ou quando passam `SELAGEM_MS` desde o **primeiro** registo (o prazo não é reiniciado).
6. Os registos continuam em `registos.log` mesmo que o chunk ainda não tenha sido selado.

Mensagens de bots e de outros canais são ignoradas. No arranque, o líder lê `registos.log`, valida a
sequência e continua a numeração a partir do último LSN (sem repetições nem saltos).

## 7. Formato de `registos.log`

Uma linha por registo:

```
lsn;instante;autor;canal;texto
```

- `lsn`: número sequencial (começa em 1).
- `instante`: milissegundos desde 1970-01-01 (epoch).
- `autor`, `canal`, `texto`: codificados individualmente em **Base64 (UTF-8)**, para que `;`, quebras de
  linha e acentos não estraguem a estrutura da linha.

Exemplo: `1;1794912000000;TWF0aWxkZQ==;YXJxdWl2bw==;T2zDoSwgbXVuZG8h`

## 8. Formato binário do chunk

Todos os inteiros são **big-endian** (`DataOutputStream`). Texto em UTF-8, precedido do seu comprimento.

Conteúdo **em claro** (antes da cifra):

```
[int  tamanhoÚtil]          4 bytes  → comprimento da serialização (sem padding)
[int  quantidade]           4 bytes  → número de registos
 para cada registo:
   [long lsn]               8 bytes
   [long instante]          8 bytes
   [int n][n bytes autor]   comprimento + UTF-8
   [int n][n bytes canal]   comprimento + UTF-8
   [int n][n bytes texto]   comprimento + UTF-8
[padding]                   bytes aleatórios (SecureRandom) até ao múltiplo seguinte de PAD_BYTES
```

### Padding

O tamanho total (prefixo + serialização + padding) é um múltiplo de `PAD_BYTES`:

```
tamanhoFinal = ceil(tamanhoOriginal / PAD_BYTES) × PAD_BYTES
```

O prefixo `tamanhoÚtil` permite saber onde acabam os dados reais, de forma que o padding é removido sem
ambiguidade (zeros no fim dos dados não se confundem com padding). O padding é aplicado **antes** da cifra.

### Leitura inversa

Lê-se o prefixo, extraem-se os `tamanhoÚtil` bytes seguintes, ignora-se o padding e reconstroem-se os
registos (LSN, instante, autor, canal, texto). Dados truncados, corrompidos ou com bytes a mais são recusados.

## 9. Cifra

### Chaves

- **MK**: chave mestra, em `master.key`.
- **DEK**: chave derivada **por chunk**. A MK nunca cifra dados diretamente.

Por chunk são gerados com `SecureRandom`: **salt** (32 bytes) e **nonce** (12 bytes).

### Derivação da DEK (HKDF-SHA256)

```
PRK = HMAC-SHA256(chave = salt, dados = MK)                      (extract)
DEK = HMAC-SHA256(chave = PRK,  dados = "tpf-chunk-v1" || 0x01)  (expand, 1 bloco → 32 bytes)
```

`info = "tpf-chunk-v1"` (ASCII).

### AES-256-GCM e AAD

- `AES/GCM/NoPadding`, chave de 256 bits, nonce de 12 bytes, tag de 128 bits.
- Dados cifrados: o conteúdo **com padding**. A tag é acrescentada pelo GCM ao texto cifrado
  (por isso um `.bin` tem `tamanhoComPadding + 16` bytes).
- **AAD** (dados autenticados, não cifrados), com `keyVersion = 1`:

```
aad = "tpf-v1" (6 bytes ASCII) || keyVersion (4 bytes, big-endian) || salt (32 bytes) || nonce (12 bytes)
```

Alterar o salt, o nonce ou a versão faz a autenticação falhar.

## 10. Chunks selados: identificação e abertura

```
chunkId = SHA-256 (em hexadecimal, 64 caracteres) do texto cifrado, incluindo a tag GCM
```

Para cada chunk são criados dois ficheiros em `dados/selados/`:

- `<chunkId>.bin`: só os bytes cifrados (texto cifrado + tag).
- `<chunkId>.hdr`: uma linha com os metadados públicos necessários para abrir o chunk:

```
keyVersion;salt;nonce
```

com `salt` e `nonce` em Base64. Exemplo: `1;HQw5CAzc72Awue...=;KYaLX3TUj4nKNzlK`

O ficheiro `.hdr` pode ser público: sem a MK não permite decifrar nada.

### Abertura (`ChunkCrypto.open`)

Verifica, **por esta ordem**:

1. o SHA-256 do conteúdo cifrado contra o `chunkId`;
2. a versão da chave;
3. a autenticação GCM (que cobre o conteúdo e o AAD).

Qualquer falha lança exceção, e nunca são devolvidos dados parcialmente decifrados. Depois remove-se o
padding e reconstroem-se os registos.

### Evento `SELADO`

Em `eventos-lider.txt`:

```
instante;SELADO;chunkId=<id>;lsnInicio=<n>;lsnFim=<m>;tamanho=<bytes com padding>
```

### Imutabilidade

Depois de selado, um chunk **nunca é alterado**: os ficheiros são criados com `CREATE_NEW` (não
substituem ficheiros existentes) e o registo seguinte abre um chunk novo.

## 11. Comandos

| Comando | Estado no Sprint E2-02 |
|---|---|
| `/registar <texto>` | Real: regista e responde com o LSN atribuído |
| `/rajada <n> <intervaloMs>` | Real (só `admin-arquivo`): injeta `n` registos sintéticos pelo mesmo método de `/registar`, com pausa de `intervaloMs` entre eles |
| `/estado`, `/config r <n>`, `/recuperar <de> <ate>`, `/replica-token`, `/replica-remover <id>` | Provisórios (sprints seguintes) |

Mensagens em `#arquivo` (de utilizadores, não de bots) são registadas pelo mesmo caminho.

## 12. Testes

`mvn test` executa:

- `ChunkCryptoTest`: ida e volta, ciphertext alterado (falha no hash), salt, nonce, `keyVersion` e
  `master.key` diferentes (falham no GCM / na versão).
- `SealedTest`: guardar um chunk e voltar a abri-lo recuperando os registos originais.

### Verificações manuais

- **Registo**: `/registar Primeiro teste` → responde com o LSN e a linha aparece em `registos.log`.
- **Reinício**: reiniciar o líder → o LSN continua a partir do último.
- **Selagem por tempo**: um único registo gera `.bin` e `.hdr` cerca de 5 s depois.
- **Selagem por tamanho**: `/rajada 500 0` gera vários chunks; todos os `.bin` têm
  `múltiplo de 4096 + 16` bytes.
- **Confidencialidade**: `grep -r "MensagemSecreta123" dados/selados/` não devolve nada.
- **Integridade**: alterar um byte de um `.bin` faz `open` falhar com exceção.