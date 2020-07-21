package com.lzj.spark.sql;

import org.apache.spark.SparkConf;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/21 14:35
 **/
public class Spark01_Test {
    public static void main(String[] args) {
        // 创建环境
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 普通逻辑操作
        //Dataset<Row> jsonDS = spark.read().json("json/person.json");
        //jsonDS.createOrReplaceTempView("person");
        //spark.sql("select * from person").show();

        // DSL
        //jsonDS.select(col("name"), col("age").plus(1).as("age")).show();
        //jsonDS.filter(col("age").equalTo(20).as("age")).show();

        // RDD <=> DataSet
        // 需要自己构建schema
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "1, lzj, 11",
                "2, lzj2, 22",
                "3, lzj3, 33"
        ));

        // 构建schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, false));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRdd = rdd.map((Function<String, Row>) v1 -> {
            String[] attr = v1.split(",");
            return RowFactory.create(attr[0], attr[1], attr[2]);
        });

        Dataset<Row> ds = spark.createDataFrame(rowRdd, schema);

        Dataset<String> stringDataset = ds.toJSON();

        stringDataset.foreach(string -> {
            System.out.println(string);
        });

        //stringDataset.show();
        //ds.show();

        spark.stop();
    }
}
