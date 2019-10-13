package CSV;

import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * Read data from csv file and write to MongoDB.
 */
public class CSVReader {

    private static final String filePath = "src/main/resources/Data";
    private static final String[] fileName = new String[]{"Beijing", "Shanghai", "Guangzhou", "Chengdu", "Shenyang"};

    /**
     * Total records: 117875
     * Total time: 19464ms
     * @param args
     */
    public static void main(String[] args) {
        int cnt = 0;
        long s = System.currentTimeMillis();
        BufferedReader[] readers = new BufferedReader[5];
        String[] records = new String[5];
        Document[] documents = new Document[5];
        DataCleaner[] cleaners = new DataCleaner[5];
        MongoUtil mongoUtil = new MongoUtil("SparkStreaming");
        try {
            for (int i = 0; i < 5; i ++){
                readers[i] = new BufferedReader(new InputStreamReader(new FileInputStream(filePath + "/" + fileName[i] + ".csv"), "UTF-8"));
            }
            for (int i = 0; i < 5; i ++){
                cleaners[i] = new DataCleaner(fileName[i]);
            }
            while (true){
                for (int i = 0; i < 5; i ++){
                    records[i] = readers[i].readLine();
                }
                if (records[0] == null ||
                        records[1] == null ||
                        records[2] == null ||
                        records[3] == null ||
                        records[4] == null){
                    break;
                }
                String[] fields = null;

                /**
                 * BeiJing
                 */
                fields = records[0].split(",");
                documents[0] = new Document("year", fields[1])
                        .append("month", fields[2])
                        .append("day", fields[3])
                        .append("hour", fields[4])
                        .append("PM", fields[9])
                        .append("humidity", fields[11])
                        .append("temperature", fields[13])
                        .append("dewp", fields[10])
                        .append("name", "Beijing");

                /**
                 * ShangHai
                 */
                fields = records[1].split(",");
                documents[1] = new Document("year", fields[1])
                        .append("month", fields[2])
                        .append("day", fields[3])
                        .append("hour", fields[4])
                        .append("PM", fields[7])
                        .append("humidity", fields[10])
                        .append("temperature", fields[12])
                        .append("dewp", fields[9])
                        .append("name", "Shanghai");

                /**
                 * GuangZhou
                 */
                fields = records[2].split(",");
                documents[2] = new Document("year", fields[1])
                        .append("month", fields[2])
                        .append("day", fields[3])
                        .append("hour", fields[4])
                        .append("PM", fields[8])
                        .append("humidity", fields[10])
                        .append("temperature", fields[12])
                        .append("dewp", fields[9])
                        .append("name", "Guangzhou");

                /**
                 * ChengDu
                 */
                fields = records[3].split(",");
                documents[3] = new Document("year", fields[1])
                        .append("month", fields[2])
                        .append("day", fields[3])
                        .append("hour", fields[4])
                        .append("PM", fields[8])
                        .append("humidity", fields[10])
                        .append("temperature", fields[12])
                        .append("dewp", fields[9])
                        .append("name", "Chengdu");

                /**
                 * ShenYang
                 */
                fields = records[4].split(",");
                documents[4] = new Document("year", fields[1])
                        .append("month", fields[2])
                        .append("day", fields[3])
                        .append("hour", fields[4])
                        .append("PM", fields[7])
                        .append("humidity", fields[10])
                        .append("temperature", fields[12])
                        .append("dewp", fields[9])
                        .append("name", "Shenyang");

                /**
                 * 数据清洗
                 */
                if (cleaners[0].clean(documents[0]) &&
                        cleaners[1].clean(documents[1]) &&
                        cleaners[2].clean(documents[2]) &&
                        cleaners[3].clean(documents[3]) &&
                        cleaners[4].clean(documents[4])){
                    for (int i = 0; i < 5; i ++){
                        mongoUtil.insert(fileName[i], documents[i]);
                    }
                    cnt = cnt + 5;
                }
            }
            long e = System.currentTimeMillis();
            System.out.println("Mission Complete!");
            System.out.println("Total records: " + cnt);
            System.out.println("Total time: " + (e - s) + "ms");
        } catch (Exception e){
            System.out.println("FAILED!");
            e.printStackTrace();
        }
    }
}
