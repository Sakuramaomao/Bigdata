package com.lzj.spark.structured.source;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * <pre>
 *    读取file文件。
 *       和RDD读取文件类似，可以直接读取指定格式的文件，但是需要指定schema。
 *
 *       load方法中执行指定文件夹，不能是某一个文件名称。
 *       已经读取过的文件不会再次被读取。
 *       新加入的文件会被立即读取。
 *
 *     file自动分区
 *       文件夹名称可以按照year=2019这样的格式嵌套，读取的时候会自动按照文件夹
 *     的名称来分区。会增加一个year字段。并且支持这种格式的文件夹嵌套。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/22 21:11
 */
public class Spark01_FileSource {
    public static void main(String[] args) throws StreamingQueryException {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("file-source-streaming")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("warn");

        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));

        StructType schema = DataTypes.createStructType(fields);

        spark.readStream()
                .format("csv")
                .schema(schema)
                .load("csv")
                .writeStream()
                .format("console")
                .start()
                .awaitTermination();

    }
}
