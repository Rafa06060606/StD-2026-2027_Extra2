
package rep03;

import java.nio.charset.StandardCharsets;

public class MedirREG {

    public static void main(String[] args) {

        String host = "127.0.0.1";
        int porto = 7000;

        // Mensagem sem carimbos causais
        String semCarimbos =
            "REG;" + host + ";" + porto + ";"
            + "1;S01;23.2;2026-10-23T14:00:00";

        // Mensagem com Lamport e vetor causal
        String comCarimbos =
            semCarimbos + ";5;1,0,0";

        int bytesSemCarimbos =
            semCarimbos.getBytes(StandardCharsets.UTF_8).length;

        int bytesComCarimbos =
            comCarimbos.getBytes(StandardCharsets.UTF_8).length;

        System.out.println(
            "REG sem carimbos: " + bytesSemCarimbos + " bytes"
        );

        System.out.println(
            "REG com carimbos: " + bytesComCarimbos + " bytes"
        );

        System.out.println(
            "Diferença: "
            + (bytesComCarimbos - bytesSemCarimbos)
            + " bytes"
        );
    }
}
