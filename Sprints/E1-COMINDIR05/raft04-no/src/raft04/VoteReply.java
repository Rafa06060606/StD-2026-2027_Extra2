package raft04;

public final class VoteReply {

    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter,
                     int candidate, boolean granted) {

        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    // Objeto -> mensagem
    public String encode() {

        return "VOTO;" +
                term + ";" +
                voter + ";" +
                candidate + ";" +
                (granted ? "S" : "N");
    }

    // Mensagem -> objeto
    public static VoteReply decode(String msg) {

        String[] f = msg.trim().split(";");

        if (f.length != 5 || !f[0].equals("VOTO")) {
            throw new IllegalArgumentException(
                    "VOTO mal formado: " + msg
            );
        }

        if (!f[4].equals("S") && !f[4].equals("N")) {
            throw new IllegalArgumentException(
                    "Decisão de voto inválida: " + f[4]
            );
        }

        return new VoteReply(
                Long.parseLong(f[1]),
                Integer.parseInt(f[2]),
                Integer.parseInt(f[3]),
                f[4].equals("S")
        );
    }
}