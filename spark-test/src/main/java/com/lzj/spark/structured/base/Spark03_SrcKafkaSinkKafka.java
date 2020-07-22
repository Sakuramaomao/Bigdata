package com.lzj.spark.structured.base;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * <pre>
 *   接收kafka数据，写入kafka。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 8:49
 **/
public class Spark03_SrcKafkaSinkKafka {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 读取Kafka数据（要求Kafka中消息为K-V类型）
        Dataset<Row> kafkaSource = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.5.134:9092")
                .option("subscribe", "streaming_test")
                // 指定读取位置。earliest、assign，latest
                .option("startingOffsets", "earliest")
                .load();

        // 只选取消息中的value。
        Dataset<Row> valueSource = kafkaSource.select("value");

        // value处理
        Dataset<String> ds = valueSource.as(Encoders.STRING()).mapPartitions(
                new MapPartitionsFunction<String, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> iter) throws Exception {
                        LinkedList<String> result = new LinkedList<>();
                        try {
                            while (iter.hasNext()) {
                                String next = iter.next();
                                JSONObject originObj = JSONObject.parseObject(next);
                                originObj.put("time", System.currentTimeMillis());
                                result.add(originObj.toJSONString());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return result.iterator();
                    }
                }
                , Encoders.STRING());

        // 其他的业务逻辑操作
        ds.createOrReplaceTempView("test");
        // 这里未定义schema，所以不能使用sparkSql
        Dataset<Row> sql = spark.sql("select * from test");

        // 写回kafka
        sql.writeStream()
                .format("kafka")
                .outputMode(OutputMode.Append())
                .option("checkpointLocation", "checkpoint")
                .option("kafka.bootstrap.servers", "192.168.5.134:9092")
                .option("topic", "streaming_test_output")
                .start()
                .awaitTermination();
    }
}
