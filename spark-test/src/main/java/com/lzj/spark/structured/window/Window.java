package com.lzj.spark.structured.window;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/22 17:29
 **/
public class Window {
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
                .writeStream()
                .format("console")
        .option("truncate", false)
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}
