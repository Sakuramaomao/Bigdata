package com.lzj.spark.structured;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.LinkedList;

import static org.apache.spark.sql.functions.*;

/**
 * <pre>
 *   接收kafka数据，写入kafka。解析json为dataset。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 8:49
 **/
public class Spark04_SrcKafkaSinkKafkaWithDS {
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
                .option("subscribe", "streaming-test")
                // 指定读取位置。earliest、assign，latest
                .option("startingOffsets", "latest")
                // 不检查
                .option("failOnDataLoss", "false")
                .load();

        // 只选取消息中的value。
        Dataset<String> valueSource = kafkaSource.select(col("value").as("value")).as(Encoders.STRING());

        // schema info
        LinkedList<StructField> fields = new LinkedList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
        StructType schema = DataTypes.createStructType(fields);

        // value处理
        Dataset<Row> ds = valueSource.mapPartitions(
                new MapPartitionsFunction<String, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> iter) throws Exception {
                        LinkedList<String> result = new LinkedList<>();
                        try {
                            while (iter.hasNext()) {
                                String next = iter.next();
                                JSONObject originObj = JSONObject.parseObject(next);
                                // 将json binary转化为json字符串
                                result.add(originObj.toJSONString());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return result.iterator();
                    }
                }
                , Encoders.STRING())
                .select(from_json(col("value"), schema).as("info"))
                .select(col("info.*"));

        // FIXME 这里value会找不到。
        //Dataset<Row> typedDS = ds.select(from_json(col("value"), schema).as("info"))
        //        .select(col("info.*"));

        // 其他的业务逻辑操作
        ds.createOrReplaceTempView("test");
        Dataset<Row> sqlDS = spark.sql("select name as value from test");

        // 写回kafka
        sqlDS.writeStream()
                .format("kafka")
                .outputMode(OutputMode.Append())
                .option("checkpointLocation", "checkpoint")
                .option("kafka.bootstrap.servers", "192.168.5.134:9092")
                .option("topic", "streaming-test-output")
                .start()
                .awaitTermination();

        // 上面阻塞了，运行不到这一行
        sqlDS.show();
    }
}
