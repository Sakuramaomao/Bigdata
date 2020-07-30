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
 *      新环境SparkSession
 *          1、read
 *              可以读取Json、Txt、Csv等类型的文件。
 *          2、RDD和Dataset之间的转换
 *              Java中没有将RDD直接转化为Dataset的API，Scala中有。
 *           在Java中需要借助sparkSession中的createDataFrame方法来创造Dataset。
 *           在使用之前需要自己手动构建Schema。
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

        JavaRDD<Row> rowRdd = rdd.map((Function<String, Row>) v1 -> {
            String[] attr = v1.split(",");
            return RowFactory.create(attr[0], attr[1], attr[2]);
        });

        // 将RDD转化为Dataset。
        Dataset<Row> ds = spark.createDataFrame(rowRdd, schema);


        //stringDataset.show();
        ds.show();

        spark.stop();
    }
}
