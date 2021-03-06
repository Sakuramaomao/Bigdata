package com.example.spark.structured.window;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;

/**
 * <pre>
 *      基于event-time的窗口操作
 *
 *      为什么是基于event-time的窗口操作？
 *          在structured streaming中可以基于事件发生时的时间对数据做聚合操作，即基于event-time
 *      的操作。
 *          在这种情况下，开发者不必考虑Spark接收数据的时间是否和事件发生的顺序一致。基于event-time
 *      的操作会大大减少开发者的工作量。
 *          在这种窗口机制下，无论事件何时到达，以怎样的顺序到达，Structured streaming总会根据事件
 *      时间生成对应的的若干个时间窗口，然后按照指定的规则聚合。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/22 17:29
 **/
public class Spark01_Window {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                // 给word自动添加时间
                .option("includeTimestamp", true)
                .load()
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap(new FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>() {
                    @Override
                    public Iterator<Tuple2<String, Timestamp>> call(Tuple2<String, Timestamp> tup) throws Exception {
                        String[] charArr = tup._1.split(" ");
                        ArrayList<Tuple2<String, Timestamp>> list = new ArrayList<>();
                        for (int i = 0; i < charArr.length; i++) {
                            String ch = charArr[i];
                            list.add(new Tuple2<>(ch, tup._2));
                        }
                        return list.iterator();
                    }
                }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .toDF("word", "timestamp")
                // { word timestamp}
                // 小练习：根据event-time开窗，根据value进行分组聚合统计
                .groupBy(
                        window(col("timestamp"), "4 minutes","2 minutes"),
                        col("word")
                )
                .count()
                .writeStream()
                .format("console")
                // 显示完整时间格式
                .option("truncate", false)
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}
