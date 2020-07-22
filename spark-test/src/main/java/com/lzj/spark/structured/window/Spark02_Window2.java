package com.lzj.spark.structured.window;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/22 17:29
 **/
public class Spark02_Window2 {
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
                .option("includeTimestamp", true)
                .load()
                .as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, Tuple2<Timestamp, String>>() {
                    @Override
                    public Iterator<Tuple2<Timestamp, String>> call(String s) throws Exception {
                        String[] strArr = s.split(",");
                        // TODO 格式化时间戳
                        //Tuple2<Timestamp, Field.Str> tup = new Tuple2<Timestamp, Field.Str>(strArr[0], strArr[1]);
                        return null;
                    }
                }, Encoders.tuple(Encoders.TIMESTAMP(), Encoders.STRING()))
                .toDF("word", "timestamp")
                .writeStream()
                .format("console")
                .option("truncate", false)
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}
