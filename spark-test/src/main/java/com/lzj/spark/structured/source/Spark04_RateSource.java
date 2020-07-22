package com.lzj.spark.structured.source;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * <pre>
 *      Rate Source
 *
 *      rate的作用
 *          rate含义为速率。是spark中自带的一个source。
 *      产生的数据结构只有两列，一列为timestamp，一列为随机整数。
 *      多用于压力测试或者自己测试使用。
 *
 *      rate也具有一些可以配置的参数（官网自己找）。比如控制每秒产生的数量、产生间隔等。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/22 22:16
 */
public class Spark04_RateSource {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("kafka-source-streaming")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("warn");

        spark.readStream()
                .format("rate")
                .load()
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                // 显示时间。默认折叠不显示。
                .option("truncate", false)
                .start()
                .awaitTermination();
    }
}
