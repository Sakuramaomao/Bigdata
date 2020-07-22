package com.lzj.spark.structured.source;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * <pre>
 *     Socket Source
 *
 *     一条warning
 *        Socket作为source时，会收到一条warn “The socket source should not be used for
 *     production applications! It does not support recovery.” 这是因为spark无法保证
 *     从socket作为source的端到端精准一次性消费exactly once。
 *        所以socket数据不适用于生产环境，只能使用于测试。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/22 22:09
 */
public class Spark03_SocketSource {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("socket-source-streaming")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("warn");

        // 小练习：读取socket中单词，直接输出都console中。
        spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}
