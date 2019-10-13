import java.io.*;
import java.net.Socket;

/**
 * This is a simulator of socket client.
 */
public class ClientSimulator {
    private static final String host = "127.0.0.1";
    private static final int port = 9999;

    public static void main(String[] args) {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            writer.write("\n");
            writer.flush();
            String line = reader.readLine();
            System.out.println(line);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
