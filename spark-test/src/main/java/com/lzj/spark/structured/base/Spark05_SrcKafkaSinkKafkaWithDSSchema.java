package com.lzj.spark.structured.base;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 18:35
 **/
public class Spark05_SrcKafkaSinkKafkaWithDSSchema {
    public static void main(String[] args) {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        Dataset<Row> textDS = spark.read().text("json/person");

        System.out.println("========textDS schema=========");
        textDS.printSchema();
        System.out.println("========textDS schema=========");

        // schema info
        LinkedList<StructField> fields = new LinkedList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
        StructType schema = DataTypes.createStructType(fields);


        Dataset<Row> typedDS = textDS.select(from_json(new Column("value"), schema).as("info"))
                .select(col("info.*"));
        System.out.println("========typedDS schema=========");
        typedDS.printSchema();
        System.out.println("========typedDS schema=========");

        typedDS.show();

    }
}
