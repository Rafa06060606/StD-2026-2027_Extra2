/*
Para que serve na tarefa

É a estrutura de dados do relógio vetorial V: um vetor com uma posição por partição (por líder). Quem a usa:

Líder: merge, tick e copy ao executar o evento.
Processador: merge e copy ao acumular conhecimento.
SensorRecord: toString e parse para escrever e ler o vetor no ficheiro e na rede.
CausalCheck: compare e get.
*/


package rep03;

import java.util.Arrays;

public final class VectorClock {

    public enum Order {
        BEFORE, AFTER, EQUAL, CONCURRENT // os 4 resultados possíveis de comparar 2 vetores
    }

    private final long[] v; // as componentes; a referência não muda, o conteúdo sim

    public VectorClock(int n) {               // vetor novo com N posições
        this.v = new long[n];                 // todas a 0: "ainda não vi nenhum evento"
    }

    private VectorClock(long[] v) {           // construtor privado: só o copy() o usa
        this.v = v;                           // adota o array que lhe dão (sem recriar a zeros)
    }

    // Atualiza cada componente com o máximo dos dois vetores.
    public void merge(VectorClock other) {    // 1.º passo da regra do líder: V[i] = max(V[i], Vmsg[i])
        if (v.length != other.v.length)  {     // a tua adição: dimensões diferentes = erro claro
            throw new IllegalArgumentException(
                "Os vetores têm dimensões diferentes."
            );
        }

        for (int i = 0; i < v.length; i++) { // fica com o que de mais recente cada processo conhece
            v[i] = Math.max(v[i], other.v[i]);
        }
    }

    // Regista um evento local na partição k.
    public void tick(int k) { // 2.º passo: evento local do líder k → V[k] = V[k]+1
        v[k]++;
    }

    public long get(int k) { // lê uma componente (usado em V[k]==seq)
        return v[k];
    }

    // Devolve uma cópia independente do vetor.
    public VectorClock copy() {    // "fotografia" independente
        return new VectorClock(v.clone()); // clone() duplica o array; alterar um não afeta o outro
    }

    // Determina a relação entre este vetor e outro.
    public Order compare(VectorClock other) { // compara this com other
        if (v.length != other.v.length) {
            throw new IllegalArgumentException(
                "Os vetores têm dimensões diferentes."
            );
        }

        boolean menor = false;
        boolean maior = false;

        for (int i = 0; i < v.length; i++) {
            if (v[i] < other.v[i]) {
                menor = true;  // há pelo menos uma componente em que this está atrás
            }

            if (v[i] > other.v[i]) {  // há pelo menos uma em que this está à frente
                maior = true;
            }
        }

        if (menor && maior) {
            return Order.CONCURRENT; // cada um sabe algo que o outro não sabe
        }

        if (menor) { 
            return Order.BEFORE; // this ≤ other em todas e < em alguma → this → other
        } 

        if (maior) {
            return Order.AFTER; // o inverso
        }
  
        return Order.EQUAL;  // iguais em todas as componentes
    }

    // Representação: v0,v1,...,vN-1
    @Override
    public String toString() {  // "1,0,3": componentes separadas por vírgula, sem espaços
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < v.length; i++) { // Percorre todas as componentes do relógio vetorial.
            if (i > 0) { // Separa os valores por vírgulas.
                sb.append(',');
            }

            sb.append(v[i]);
        }

        return sb.toString(); // Exemplo: [1, 2, 0] passa a "1,2,0".
    }

    public static VectorClock parse(String s, int n) { // inverso do toString
        String[] f = s.trim().split(",");

        if (f.length != n) {
            throw new IllegalArgumentException( // valida a dimensão (ex.: N=4 vs N=3)
                "Vetor com dimensão " + f.length
                + ", esperado " + n + ": " + s
            );
        }

        long[] v = new long[n];

        for (int i = 0; i < n; i++) {
            v[i] = Long.parseLong(f[i]); // texto não numérico → NumberFormatException (é uma IAE)
        }

        return new VectorClock(v);
    }

    @Override
    public boolean equals(Object o) {   // igualdade por conteúdo (Arrays.equals), não por referência
        return o instanceof VectorClock
            && Arrays.equals(v, ((VectorClock) o).v);
    }

    @Override
    public int hashCode() {   // igualdade por conteúdo (Arrays.equals), não por referência
        return Arrays.hashCode(v); 
    }
}
