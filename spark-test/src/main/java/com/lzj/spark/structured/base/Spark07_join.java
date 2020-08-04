package com.lzj.spark.structured.base;

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
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * <pre>
 *    实时和离线数据的join。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/24 13:39
 **/
public class Spark07_join {
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

        Dataset<Row> studentDs = spark.read().jdbc(url, "student", getProperty());
        studentDs.createOrReplaceTempView("student");

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
        Dataset<Row> dynamicDS = valueSource.mapPartitions(
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

        // 其他的业务逻辑操作, 可以使用sparkSql来返回其他streaming dynamicDS
        // src
        dynamicDS.createOrReplaceTempView("test");
        // mapping
        Dataset<Row> result = spark.sql("select student.* from test inner join student where test.name = student.name");
        result.createOrReplaceTempView("result");
        // sink
        Dataset<Row> sinkDS = spark.sql("select * from result");

        // 将每行记录转化为json字符串，准备写回kafka
        //Dataset<String> jsonDS = result.toJSON();

        // 写到console
        sinkDS.writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                //.outputMode(OutputMode.Append())
                //.option("checkpointLocation", "checkpoint")
                //.option("kafka.bootstrap.servers", "192.168.5.134:9092")
                //.option("topic", "streaming-test-output")
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
