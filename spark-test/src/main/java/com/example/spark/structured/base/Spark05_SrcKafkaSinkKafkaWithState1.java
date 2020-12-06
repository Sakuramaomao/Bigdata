package com.example.spark.structured.base;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

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
public class Spark05_SrcKafkaSinkKafkaWithState1 {
    public static final String url = "jdbc:mysql://192.168.5.131:3306/fyh";
    public static final String userName = "root";
    public static final String passWord = "123456";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";


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
                .writeStream()
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> ds, Long v2) throws Exception {
                        ds.write().mode(SaveMode.Overwrite).jdbc(url, "streaming_test", getProperty());
                    }
                })
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }

    private static Properties getProperty() {
        Properties prop = new Properties();
        prop.setProperty("user", userName);
        prop.setProperty("password", passWord);
        prop.setProperty("driver", MYSQL_DRIVER_NAME);
        return prop;
    }
}
