package com.lzj.spark.structured;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/20 19:34
 **/
public class Spark01_SocketWordCount {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> source = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> sourceDS = source.as(Encoders.STRING());

        Dataset<String> words = sourceDS.flatMap((FlatMapFunction<String, String>)
                s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
