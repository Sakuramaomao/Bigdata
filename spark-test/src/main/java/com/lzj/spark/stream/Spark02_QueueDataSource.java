package com.lzj.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *     测试用，从队列中采集数据。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/20 13:46
 **/
public class Spark02_QueueDataSource {
    public static void main(String[] args) throws InterruptedException {
        // 配置（Streaming使用的核至少是2，当核为1时，Receiver将不会运行）
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming");
        // 初始化环境
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));

        Queue<JavaRDD<String>> rddQueue = new LinkedList<>();
        JavaDStream<String> queueStream = ssc.queueStream(rddQueue);

        queueStream.print();

        ssc.start();

        // FIXME 不成功，无法采集到队列数据。
        for (int i = 0; i < 5; i++) {
            rddQueue.add(ssc.sparkContext().parallelize(Arrays.asList("a", "b", "c")));
            TimeUnit.SECONDS.sleep(3);
        }

        ssc.awaitTermination();
    }
}
