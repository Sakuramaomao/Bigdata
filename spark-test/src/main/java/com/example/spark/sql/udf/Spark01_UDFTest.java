package com.example.spark.sql.udf;

import com.example.spark.sql.sampleclass.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

/**
 * <pre>
 *     UDF
 *      1、UDF的使用
 *          UDF...等都是函数式接口。UDF1表示输入一个值返回一个值。
 *      2、为什么使用UDF？
 *          DataFrame<Row>中的Row使用会报错，所以不会直接操作Row。
 *          Dataset<User>中的User可以直接操作，使用算子操作比较麻烦，而注册UDF在SQL中使用比较方便。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/8/4 22:40
 */
public class Spark01_UDFTest {
    public static void main(String[] args) {
        // 创建环境
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<User> rdd = sc.parallelize(Arrays.asList(
                "1,lzj,11",
                "2,lzj2,12",
                "3,lzj3,13"
        )).map(str -> {
            String[] attr = str.split(",");
            User user = new User();
            user.setId(Integer.parseInt(attr[0]));
            user.setName(attr[1]);
            user.setAge(Integer.parseInt(attr[2]));
            return user;
        });

        Dataset<User> ds = spark.createDataset(rdd.rdd(), Encoders.bean(User.class));
        ds.createOrReplaceTempView("test");

        // 注册并使用udf
        spark.udf().register("addName", (UDF1<String, String>) s -> "name: " + s, DataTypes.StringType);
        spark.sql("select name from test").show();
        spark.sql("select addName(name) name from test").show();
    }
}
