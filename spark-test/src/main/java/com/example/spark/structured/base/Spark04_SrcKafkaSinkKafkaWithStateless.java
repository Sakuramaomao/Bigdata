package com.example.spark.structured.base;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.LinkedList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * <pre>
 *   <b>无状态实时</b>
 *
 *   接收kafka数据，写入kafka。
 *
 *     1、输入：JSON对象字符串（不支持JSON对象嵌套，不支持JSONArray对象）
 *         {"name": "lzj88888", "age": 29}
 *
 *     2、Query：sparkSql
 *          流式DataSet支持基本操作，基本操作不包括聚合操作。基本操作包括大部分的静态DataFrame操作，
 *       * 有条件支持的操作
 *         sparkSql的select、where、join、groupBy，RDD的map、filter和flatMap操作。
 *       * 不支持
 *         a、Dataset的full outer join
 *         b、在右侧使用Dataset的left outer join
 *         c、在左侧使用Dataset的right outer join
 *         d、两种Dataset之间的任何种类join操作
 *         e、将Dataset转化为RDD再进行操作
 *
 *     3、输出：以JSON对象字符串的方式写回Kafka。
 *       写出模式
 *       * Append（结果表中永远不会变得数据会被丢弃，因为sink出去了，没必要保存）
 *         追加模式。只输出那些将来永远不可能发生变化的数据。
 *         没有聚合的时候，append和update一致。
 *         有聚合的时候，一定要有watermark水印，才能使用append。watermark标记了哪些数据不会发生变化。
 *       * Complete（结果表数据永远不会丢弃）
 *         每次都输出所有数据的计算结果。
 *         一批数据中必须有聚合操作才能使用Complete模式。
 *         如果没有聚合操作，那么给stateStore造成的压力比较大。
 *       * Update（结果表中不会保存数据，都sink出去了）
 *         只输出变化的部分，比如只输出结果表中最新得到的数据。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 8:49
 **/
public class Spark04_SrcKafkaSinkKafkaWithStateless {
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
                .option("kafka.bootstrap.servers", "192.168.5.132:9092")
                .option("subscribe", "streamingtest")
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

        // 其他的业务逻辑操作, 可以使用sparkSql来返回其他streaming ds
        ds.createOrReplaceTempView("test");
        Dataset<Row> sqlDS = spark.sql("select name as myName, age + 1 as myAge from test");

        // 将每行记录转化为json字符串，准备写回kafka
        Dataset<String> jsonDS = sqlDS.toJSON();

        // 判断DS是否为streaming DS的方法
        //System.out.println(jsonDS.isStreaming());

        jsonDS.writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .option("checkpointLocation", "checkpoint")
                .option("truncate", false)
                .start()
                .awaitTermination();

        // 写回kafka
        //jsonDS.writeStream()
        //        .format("kafka")
        //        .outputMode(OutputMode.Append())
        //        .option("checkpointLocation", "checkpoint")
        //        .option("kafka.bootstrap.servers", "192.168.5.132:9092")
        //        .option("topic", "streamingtestoutput")
        //        .start()
        //        .awaitTermination();
    }
}
