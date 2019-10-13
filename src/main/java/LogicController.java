import CSV.MongoUtil;
import org.bson.Document;

import java.io.OutputStream;
import java.net.Socket;

/**
 * This is the source code as a client.
 */
public class LogicController {
    private static final String host = "127.0.0.1";
    private static final int port = 9999;
    public static void main(String[] args) {
        try {
            long s = System.currentTimeMillis();
            Socket socket = new Socket(host, port);
            OutputStream outputStream = socket.getOutputStream();
            MongoUtil mongoUtil = new MongoUtil("SparkStreaming");
            while (true){
                Document document = mongoUtil.selectNext("Beijing");
                if (document == null){
                    break;
                }
                outputStream.write((document.toJson() + "\n").getBytes("UTF-8"));
                Thread.sleep(1000);
            }
            socket.close();
            long e = System.currentTimeMillis();
            System.out.println("Mission Complete!");
            System.out.println("Total time: " + ((e - s) / 1000) + "s");
        } catch (Exception e){
            System.out.println("FAILED!");
            e.printStackTrace();
        }
    }

    private static String format(Document document){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("---------------------------------------------------\n");
        stringBuilder.append("\"year\" : ").append(document.get("year")).append("\n");
        stringBuilder.append("\"month\" : ").append(document.get("month")).append("\n");
        stringBuilder.append("\"day\" : ").append(document.get("day")).append("\n");
        stringBuilder.append("\"hour\" : ").append(document.get("hour")).append("\n");
        stringBuilder.append("\"PM2.5\" : ").append(document.get("PM")).append("\n");
        stringBuilder.append("\"Temperature\" : ").append(document.get("temperature")).append("\n");
        stringBuilder.append("\"Humidity\" : ").append(document.get("humidity")).append("\n");
        stringBuilder.append("---------------------------------------------------\n");
        return stringBuilder.toString();
    }
}
