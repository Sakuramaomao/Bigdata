package com.lzj.spark.structured;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * <pre>
 *   接收kafka数据，写入本地文件。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 8:49
 **/
public class Spark02_SrcKafkaSinkFile {
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

        /**
         * schema
         *  -> key: 消息key
         *  -> value: 消息value
         *  -> topic: 本条消息所在topic。因为整合的时候一个dataset可以对接多个topic。
         *  -> partition: 本条消息所在分区号。
         *  -> offset: 本条消息在分区中的偏移量。
         *  -> timestamp: 本条消息进入kafka的时间戳。
         *  -> timestampType: 时间戳类型。
         */
        //kafkaSource.printSchema();

        Dataset<Row> valueSource = kafkaSource.select("value");

        // 处理数据
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

        // 1、一条消息的格式  1::lzj1::test1
        // 2、将其当做CSV来处理 Dataset<String> => Dataset(id, name, info)
        //Dataset<Row> sourceRow = source.map((Function1<String, Row>) item -> {
        //    String[] attrs = item.split("::");
        //    return RowFactory.create(attrs[0], attrs[1], attrs[2]);
        //}, Encoders.bean(Row.class));
        //
        //Dataset<Row> result = sourceRow.toDF("id", "name", "age");

        ds.createOrReplaceTempView("test");
        Dataset<Row> sql = spark.sql("select * from test");

        // 落地HDFS
        sql.writeStream()
                .format("parquet")
                .option("path", "output")
                .option("checkpointLocation", "checkpoint")
                .start()
                .awaitTermination();

    }
}
