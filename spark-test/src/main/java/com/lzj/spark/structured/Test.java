package com.lzj.spark.structured;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 18:35
 **/
public class Test {
    public static void main(String[] args) {
        // 准备环境
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "{\"name\": \"lzj2\", \"age\": 29}",
                "{\"name\": \"lzj2\", \"age\": 29}",
                "{\"name\": \"lzj2\", \"age\": 29}"
        ));

        JavaRDD<Row> rowRdd = rdd.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                return RowFactory.create(v1);
            }
        });

        // schema info
        LinkedList<StructField> fields = new LinkedList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.createDataFrame(rowRdd, schema);

        df.printSchema();

        //Dataset<Row> typedDS = df.select(from_json(new Column("value"), schema).as("info"))
        //        .select(explode(new Column("info").as("info")))
        //        .select(col("info.name"), col("info.age"));
    }
}
