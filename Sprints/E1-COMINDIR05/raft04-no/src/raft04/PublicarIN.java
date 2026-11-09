package raft04;

// Ferramenta de teste: publica uma leitura IN na partição certa.
//   java raft04.PublicarIN <sensor> <temperatura> [N] [api]
public class PublicarIN {
    public static void main(String[] args) throws Exception {
        String sensor = args[0];
        double temp = Double.parseDouble(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        int k = new Partitioner(n).of(sensor);
        String msg = Wire.in(sensor, temp, 0, new VectorClock(n));
        new IpfsPubSub(api).publish(Wire.topicIn(k), msg);
        System.out.println("Publicado em " + Wire.topicIn(k) + ": " + msg);
    }
}