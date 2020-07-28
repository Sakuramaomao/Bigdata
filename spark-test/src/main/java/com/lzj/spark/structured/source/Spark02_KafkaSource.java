package com.lzj.spark.structured.source;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;

/**
 * <pre>
 *     Kafka Source
 *
 *     消息格式
 *        从kafka消费的消息格式必须是K-V结构。
 *
 *     从kafka读取到的DS schema
 *       * key（binary）   消息的key。如果消息中只有value，那么key为null。
 *       * value（binary）  消息的value。注意和key一样都是字节。一般都是先变为string再使用。
 *       * topic（String）  消息所在的topic。
 *       * partition（int）  消息所在的分区号。
 *       * offset（long）   消息所在分区中的偏移量。
 *       * timestamp（timestamp） 消息写入kafka的时间。
 *       * timestampType（int） 时间戳类型。
 *
 *
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/22 21:35
 */
public class Spark02_KafkaSource {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("kafka-source-streaming")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("WARN");

        // 小练习1：直接打印input table，了解DS的schema结构。
        //spark.readStream()
        //        .format("kafka")
        //        .option("kafka.bootstrap.servers", "192.168.2.10:9092")
        //        .option("subscribe", "streaming-test-in")
        //        .load()
        //        .writeStream()
        //        .format("console")
        //        .outputMode(OutputMode.Update())
        //        .start()
        //        .awaitTermination();

        // 小练习2：从kafka读取单词的wordCounts。
        spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.5.132:9092")
                .option("subscribe", "streamingtest")
                .load()
                .selectExpr("cast(value as string) as value")
                .as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        String[] strArr = s.split(" ");
                        return Arrays.asList(strArr).iterator();
                    }
                }, Encoders.STRING())
                .groupBy(col("value"))
                .count()
                .writeStream()
                .trigger(Trigger.ProcessingTime(100))
                .format("console")
                // 因为词频统计中使用到了聚合操作，所以这里使用complete模式。
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }
}
