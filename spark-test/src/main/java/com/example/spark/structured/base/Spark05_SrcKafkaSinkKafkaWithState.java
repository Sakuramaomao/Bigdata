package com.example.spark.structured.base;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;

/**
 * <pre>
 *   <b>有状态实时。即slideWindow</b>
 *
 *   接收kafka数据，写入kafka。
 *
 *     输入：JSON对象字符串（不支持JSON对象嵌套，不支持JSONArray对象）
 *         {"name": "lzj88888", "age": 29}
 *
 *     Query：sparkSql
 *
 *     输出：以JSON对象字符串的方式写回Kafka。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 8:49
 **/
public class Spark05_SrcKafkaSinkKafkaWithState {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 读取Kafka数据（要求Kafka中消息为K-V类型）
        spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.5.134:9092")
                .option("subscribe", "streaming-test")
                // 指定读取位置。earliest、assign，latest
                .option("startingOffsets", "latest")
                // 不检查
                .option("failOnDataLoss", "false")
                .load()
                .select(col("value").as("word"))
                .as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        String[] strArr = s.split(" ");
                        return Arrays.asList(strArr).iterator();
                    }
                }, Encoders.STRING())
                .select(col("value").as("word"))
                .groupBy(col("word"))
                .count()
                .toJSON()
                .writeStream()
                .format("kafka")
                .outputMode(OutputMode.Complete())
                .option("checkpointLocation", "checkpoint")
                .option("kafka.bootstrap.servers", "192.168.5.134:9092")
                .option("topic", "streaming-test-output")
                .start()
                .awaitTermination();
    }
}
