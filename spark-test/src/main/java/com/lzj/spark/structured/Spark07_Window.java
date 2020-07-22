package com.lzj.spark.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * <pre>
 *   Window
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/22 10:31
 **/
public class Spark07_Window {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        // timestamp value
        Dataset<Row> rateDS = spark.readStream()
                .format("rate")
                .load();

        // 简单处理
        Dataset<Row> result = rateDS;

        result.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime("20 seconds"))
                .start()
                .awaitTermination();
    }
}
