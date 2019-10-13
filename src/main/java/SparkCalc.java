import okhttp3.*;
import com.alibaba.fastjson.*;
import org.apache.commons.math3.exception.MathIllegalStateException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import com.cloudera.sparkts.models.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SparkCalc {

    private static OkHttpClient client;

    private static String appName = "PM2P5R";
    private static String master = "local[2]";
    private static String host = "localhost";
    private static int port = 9999;

    private static int count = 0;
    private static double sumPM = 0.0;
    private static double tmpValue = 0.0;
    private static List<Double> pmList = new ArrayList<>();

    private static String tmpYear = "";
    private static String tmpMonth = "";
    private static String tmpDay = "";
    private static String lastHour = "";
    private static String tmpHour = "";

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
                        pmList.add(SparkCalc.getTmpValue());
                    }
                }

                String[] timeKeys = {"year", "month", "day", "hour"};
                for (final String timeKey : timeKeys) {
                    JavaPairRDD<String, String> pairRDD = rdd.mapToPair(new PairFunction<JSONObject, String, String>() {
                        public Tuple2<String, String> call(JSONObject obj) throws Exception {
                            return new Tuple2<String, String>(timeKey, (String) obj.get(timeKey));
                        }
                    });

                    pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
                        public void call(Tuple2<String, String> tuple2) throws Exception {
                            switch (tuple2._1()) {
                                case "year":
                                    SparkCalc.setTmpYear(tuple2._2());
                                    break;
                                case "month":
                                    SparkCalc.setTmpMonth(tuple2._2());
                                    break;
                                case "day":
                                    SparkCalc.setTmpDay(tuple2._2());
                                    break;
                                case "hour":
                                    SparkCalc.setLastHour(SparkCalc.getTmpHour());
                                    SparkCalc.setTmpHour(tuple2._2());
                                    break;
                                default:
                                    break;
                            }
                        }
                    });
                }
                String time = tmpYear + "-" + tmpMonth + "-" + tmpDay + " " + tmpHour + ":00:00";
                result.put("time", time);

                int width = 180;
                int startIdx = pmList.size() - width - 1;
                int predictN = 1;
                double[] trainSet = new double[width];
                double predict = 0.0;
                if (startIdx >= 0) {
                    for (int i = 0; i < width; i++) {
                        trainSet[i] = pmList.get(startIdx + i);
                    }
                    Vector ts = Vectors.dense(trainSet);
                    Vector res = hwForecast(ts, predictN, 72);
                    if (res == null) {
                        predict = result.getDouble("PM");
                    } else {
                        predict = res.toArray()[0];
                    }
                    result.put("predict", predict);
                } else {
                    result.put("predict", result.getDouble("PM"));
                }

                if (count < 7) {
                    result.put("average pm2.5", result.getDouble("PM"));
                } else {
                    double sumPM = 0.0;
                    for (int i = 0; i < 7; i++) {
                        sumPM += pmList.get(pmList.size() - i - 1);
                    }
                    result.put("average pm2.5", sumPM / 7);
                }

                if (result.getDouble("average pm2.5") != 0 && !(tmpYear.equals(tmpMonth) && tmpYear.equals(tmpDay)) && !lastHour.equals(tmpHour)) {
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

    /**
     * 使用Holt-Winter模型对时间序列进行拟合
     *
     * @param ts         time series, 按时间排列的属性值，本项目中为pm2.5
     * @param predictedN 预测值的个数
     * @param period     时间序列的周期(需要人工估计)
     * @return 按顺序排列的预测值
     */
    private static Vector hwForecast(Vector ts, int predictedN, int period) {
        try {
            HoltWintersModel model = HoltWinters.fitModelWithBOBYQA(ts, period, "additive");
            Vector dest = Vectors.zeros(predictedN);
            model.forecast(ts, dest);
            return dest;
        } catch (MathIllegalStateException e) {
            //存在计算失败的情况，需要考虑怎么填入无法预测的值
            e.printStackTrace();
            return null;
        }
    }

    public static double getTmpValue() {
        return tmpValue;
    }

    public static void setTmpValue(double tmpValue) {
        SparkCalc.tmpValue = tmpValue;
    }

    public static String getTmpYear() {
        return tmpYear;
    }

    public static void setTmpYear(String tmpYear) {
        SparkCalc.tmpYear = tmpYear;
    }

    public static String getTmpMonth() {
        return tmpMonth;
    }

    public static void setTmpMonth(String tmpMonth) {
        SparkCalc.tmpMonth = tmpMonth;
    }

    public static String getTmpDay() {
        return tmpDay;
    }

    public static void setTmpDay(String tmpDay) {
        SparkCalc.tmpDay = tmpDay;
    }

    public static String getLastHour() {
        return lastHour;
    }

    public static void setLastHour(String lastHour) {
        SparkCalc.lastHour = lastHour;
    }

    public static String getTmpHour() {
        return tmpHour;
    }

    public static void setTmpHour(String tmpHour) {
        SparkCalc.tmpHour = tmpHour;
    }

}
