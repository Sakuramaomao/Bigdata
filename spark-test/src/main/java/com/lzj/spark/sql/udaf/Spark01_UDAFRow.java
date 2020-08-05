package com.lzj.spark.sql.udaf;

import com.lzj.spark.sql.sampleclass.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * <pre>
 *    UDAF(自定义聚合函数) - 弱类型版的UDAF
 *    1、新版的改变
 *     在3.x.x版本，{@link UserDefinedAggregateFunction} 被弃用，取而代之的是{@link Aggregator}。
 *     因为前者是直接操作Row，没有类型，写的时候容易出错，后者是强类型的，写的时候不容易出错。
 *    2、重写方法的注意事项
 *      * buffer继承自Row，用Row来缓冲聚合的数据。
 *      * Row的操作是update和get。
 *      * 和累加器类似，累加器有分区内聚合、各分区间合并的概念。缓冲区是分布式的，有多个缓冲器，最后有缓冲区合并的概念。
 *    3、小练习
 *      求年龄平均值。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/8/5 10:05
 **/
public class Spark01_UDAFRow {
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

        // 注册UDAF
        spark.udf().register("avgAge", new MyAvg());
        spark.sql("select avgAge(age) from test").show();
    }
}

class MyAvg extends UserDefinedAggregateFunction {

    // 输入数据的结构信息：年龄信息
    @Override
    public StructType inputSchema() {
        StructType inputSchema = new StructType(new StructField[]{
                DataTypes.createStructField("age", DataTypes.LongType, false)
        });
        return inputSchema;
    }

    // 缓冲区的数据结构信息：年龄的总和，人的数量
    @Override
    public StructType bufferSchema() {
        StructType bufferSchema = new StructType(new StructField[]{
                DataTypes.createStructField("totalAge", DataTypes.LongType, false),
                DataTypes.createStructField("count", DataTypes.LongType, false)
        });
        return bufferSchema;
    }

    // 聚合函数返回的结果类型
    @Override
    public DataType dataType() {
        return DataTypes.LongType;
    }

    // 自定义函数的幂等性。输入相同的值是否返回相同的结果。
    @Override
    public boolean deterministic() {
        return true;
    }

    // 缓冲区的初始值。buffer继承自Row。也就是一行。
    // 初始时totalAge和count都是0.
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        // 更新Row，使用update。第一个参数是下标，第二个参数是当前下标要被设置的值。
        buffer.update(0, 0L);
        buffer.update(1, 0L);
    }

    // 更新缓冲区的数据。（每个计算节点上的缓冲区）
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        long age = buffer.getLong(0) + input.getLong(0);
        long count = buffer.getLong(1) + 1;
        buffer.update(0, age);
        buffer.update(1, count);
    }

    // 合并不同节点上的缓冲区。
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        // 求age总和，求count总和。
        buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
    }

    // 缓冲区计算结果。
    @Override
    public Object evaluate(Row buffer) {
        // totalAge / count
        return buffer.getLong(0) / buffer.getLong(1);
    }
}
