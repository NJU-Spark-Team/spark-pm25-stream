import CSV.MongoUtil;
import org.bson.Document;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This is the source code as a server.
 */
public class LogicServer {
    public static MongoUtil mongoUtil = new MongoUtil("SparkStreaming");
    public static void main(String[] args) {
        new LogicServer().startAction();
    }
    private void startAction() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(9999);
            System.out.println("Mission Ready!");
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new socketServerThread(socket)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    class socketServerThread implements Runnable{
        Socket socket;
        BufferedReader reader;
        BufferedWriter writer;

        public void run() {
            try {
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

                while (true){
                    Document document = LogicServer.mongoUtil.selectNext("Beijing");
                    System.out.println(document.toJson());
                    writer.write(document.toJson() + "\n");
                    writer.flush();
                    try {
                        Thread.sleep(900L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e){
    //            e.printStackTrace();
            } finally {
                try {
                    if (reader != null){
                        reader.close();
                    }
                    if (writer != null){
                        writer.close();
                    }
                    if (socket != null){
                        socket.close();
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        public socketServerThread(Socket socket){
            super();
            this.socket = socket;
        }
    }
}

