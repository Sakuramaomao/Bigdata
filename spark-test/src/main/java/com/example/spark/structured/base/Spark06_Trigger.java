package com.example.spark.structured.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * <pre>
 *   1、微批次（默认是微批次）
 *     structured streaming默认会运行在微批次模式下。当一个批次结束后，立刻开始下一个批次。之间没有等待时间。
 *     即尽快处理数据。
 *
 *   2、连续流处理（使用特殊的Trigger完成功能）
 *        writeStream时通过trigger API来指定使用Trigger.Continuous("1 second")
 *     来激活连续流处理。
 *
 *     限制：
 *     * 只支持Map类的有类型操作
 *     * 只支持普通的SQL类操作，不支持聚合
 *     * Source只有Kafka
 *     * Sink只有Kafka，console和Memory
 *
 *    3、小技巧
 *        Spark为Streaming提供了一个默认source，用于测试。这个source是KV结构，K是时间戳
 *      V是一个随机数。
 *        readStream时通过format API来制定使用“rate”格式source即可。
 *
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/22 10:31
 **/
public class Spark06_Trigger {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

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
