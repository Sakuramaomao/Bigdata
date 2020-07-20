package com.lzj.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/20 11:13
 **/
public class Spark01_WordCount {
    public static void main(String[] args) throws InterruptedException {
        // 配置（Streaming使用的核至少是2，当核为1时，Receiver将不会运行）
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming");
        // 初始化环境
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));

        // 监控端口，创建DStream，读入的数据为一行一行的
        JavaReceiverInputDStream<String> lineStream = ssc.socketTextStream("localhost", 9999);

        // 将每行切分为一个一个单词
        JavaDStream<String> words = lineStream.flatMap(
                s -> Arrays.asList(s.split(" ")).iterator());

        // 改变单词结构，映射为元组
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        // 分组聚合。
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        // 打印
        wordCounts.print();

        // Driver程序在执行Streaming过程中不能停止。
        // Receiver(采集器)在正常情况下启动后不应该停止，除非特殊情况。

        // 启动采集器
        ssc.start();

        // 等等采集器结束
        ssc.awaitTermination();
    }
}
