import okhttp3.*;
import com.alibaba.fastjson.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;

public class SparkCalc {

    private static OkHttpClient client;

    private static String appName = "PM2P5R";
    private static String master = "local[2]";
    private static String host = "localhost";
    private static int port = 9999;

    private static int count = 0;
    private static double sumPM = 0.0;
    private static double tmpValue = 0.0;
    private static String tmpStamp = "";

    public static void main(String[] args) throws Exception {
//        if (args.length < 4) {
//            System.err.println("Usage: PM2P5R <source-hostname> <source-port>  <dest-hostname> <dest-port>");
//            System.exit(1);
//        }

        client = new OkHttpClient();
//        final String url = "http://" + args[2] + ":" + args[3] + "/submit";

        final String url = "http://localhost:8090/submit";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");

        // 初始化sparkConf
        SparkConf sparkConf = new SparkConf().setMaster(master).setAppName(appName);

        // 获得JavaStreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // 从socket源获取数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, port);

        // 转为json类型
        JavaDStream<JSONObject> jsonObjects = lines.flatMap(new FlatMapFunction<String, JSONObject>() {
            public Iterator<JSONObject> call(String str) throws Exception {
                return Collections.singletonList(JSON.parseObject(str)).iterator();
            }
        });

        jsonObjects.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            public void call(JavaRDD<JSONObject> rdd) throws Exception {
                JSONObject result = new JSONObject();
//                String[] keys = {"time", "temperature", "humidity", "PM", "average pm2.5"};

                String[] keys = {"temperature", "humidity", "PM"};
                for (final String key : keys) {
                    JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<JSONObject, String, Double>() {
                        public Tuple2<String, Double> call(JSONObject obj) throws Exception {
                            return new Tuple2<String, Double>(key, Double.parseDouble((String) obj.get(key)));
                        }
                    }).reduceByKey(new Function2<Double, Double, Double>() {
                        public Double call(Double pm, Double tmpPm) throws Exception {
                            return pm + tmpPm;
                        }
                    });

                    pairRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                        public void call(Tuple2<String, Double> tuple2) throws Exception {
                            SparkCalc.setTmpValue(tuple2._2());
                        }
                    });

                    result.put(key, SparkCalc.getTmpValue());

                    if (key.equals("PM")) {
                        count++;
                        sumPM += SparkCalc.getTmpValue();
                    }
                }
                if (count != 0) {
                    result.put("average pm2.5", sumPM / count);
                } else {
                    result.put("average pm2.5", 0);
                }

                String[] timeKeys = {"year", "month", "day", "hour"};
                String time = "";
                for (final String timeKey : timeKeys) {
                    JavaPairRDD<String, String> pairRDD = rdd.mapToPair(new PairFunction<JSONObject, String, String>() {
                        public Tuple2<String, String> call(JSONObject obj) throws Exception {
                            return new Tuple2<String, String>(timeKey, (String) obj.get(timeKey));
                        }
                    });

                    pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
                        public void call(Tuple2<String, String> tuple2) throws Exception {
                            SparkCalc.setTmpStamp(tuple2._2());
                        }
                    });

                    time += SparkCalc.getTmpStamp();
                    switch (timeKey) {
                        case "year":
                            time += "-";
                            break;
                        case "month":
                            time += "-";
                            break;
                        case "day":
                            time += " ";
                            break;
                        case "hour":
                            time += ":00:00";
                            break;
                        default:
                            break;
                    }
                }

                result.put("time", time);
                if (result.getDouble("average pm2.5") != 0 && result.getString("time").length() >= 16) {
                    post(url, result.toJSONString());
                    System.out.println(result);
                }
            }
        });


        // 开始作业
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ssc.close();
        }
    }

    /**
     * post method to server
     *
     * @param url     dest url
     * @param jsonStr String representation of json object
     */
    public static void post(String url, String jsonStr) {
        RequestBody body = RequestBody.create(jsonStr, MediaType.parse("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try {
            client.newCall(request).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double getTmpValue() {
        return tmpValue;
    }

    public static void setTmpValue(double tmpValue) {
        SparkCalc.tmpValue = tmpValue;
    }

    public static String getTmpStamp() {
        return tmpStamp;
    }

    public static void setTmpStamp(String tmpStamp) {
        SparkCalc.tmpStamp = tmpStamp;
    }

}
