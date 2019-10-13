import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SparkSocket {
    static ServerSocket serverSocket = null;
    static PrintWriter pw = null;

    public static void main(String[] args) {
        try {
            serverSocket = new ServerSocket(9999);
            System.out.println("服务启动，等待连接");
            Socket socket = serverSocket.accept();
            System.out.println("连接成功，来自：" + socket.getRemoteSocketAddress());
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            int j = 0;
            while (j < 10000) {
                j++;
                String str = "{ \"_id\" : { \"$oid\" : \"5d9ef995a0419f139c5e60f2\" }, \"year\" : \"2013\", \"month\" : \"4\", \"day\" : \"23\", \"hour\" : \"17\", \"PM\" : \"258\", \"humidity\" : \"62\", \"temperature\" : \"15\", \"name\" : \"Beijing\" }";
                pw.println(str);
                pw.flush();
                System.out.println(str);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
