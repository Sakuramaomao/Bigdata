package com.lzj.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *      新环境SparkSession
 *          1、read
 *              可以读取Json、Txt、Csv等类型的文件。
 *          2、RDD和DataFrame之间的转换
 *              DataFrame是一种特殊的Dataset。type DataFrame = Dataset[Row]
 *
 *              从DataFrame转化为RDD，只需要调用toJavaRDD方法即可获取RDD。
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
        // 从这里可以看出RDD只有数据，没有结构和类型。
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

        //-----------------------------------------------------
        JavaRDD<Row> rdd1 = rdd.map((Function<String, Row>) v1 -> {
            String[] attr = v1.split(",");
            return RowFactory.create(attr[0], attr[1], attr[2]);
        });
        // 将RDD转化为DataFrame。
        Dataset<Row> df = spark.createDataFrame(rdd1, schema);

        df.show();

        // 将DataFrame转化为RDD
        JavaRDD<Row> javaRDD = df.toJavaRDD();

        //-----------------------------------------------------
        JavaRDD<User> rdd2 = rdd.map((Function<String, User>) str -> {
            String[] attr = str.split(",");
            User u = new User();
            u.id = Integer.parseInt(attr[0]);
            u.name = attr[1];
            u.age = Integer.parseInt(attr[2]);
            return u;
        });
        // 将RDD转化为Dataset。需要样例类。
        //spark.createDataset(rdd2, Encoders.bean(User.class));
        // 将Dataset转化为RDD。

        spark.stop();
    }
}

class User {
    int id;
    String name;
    int age;
}
