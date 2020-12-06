package com.example.spark.sql.udaf;

import com.example.spark.sql.sampleclass.AgeBuffer;
import com.example.spark.sql.sampleclass.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;

import java.util.Arrays;

/**
 * <pre>
 *  UDAF（自定义函数） - 强类型版。
 *   1、新版的改变
 *    在3.x.x版本，{@link UserDefinedAggregateFunction} 被弃用，取而代之的是{@link Aggregator}。
 *    因为前者是直接操作Row，没有类型，写的时候容易出错，后者是强类型的，写的时候不容易出错。
 *   2、强类型udaf和弱类型udf区别
 *    * 需要额外使用样例类。
 *    * 强类型的udaf无法直接在sparkSql中使用，只能配合DSL来使用。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/8/5 11:27
 **/
public class Spark02_UDAFStrongType {
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
        ds.show();

        // 注册udaf
        MyAvg2 myAvg2 = new MyAvg2();
        // 强类型的udaf和弱类型的udf使用方式不一样。
        TypedColumn<User, Long> myAvg = myAvg2.toColumn().name("myAvg2");
        Dataset<Long> result = ds.select(myAvg);
        result.show();
    }
}

class MyAvg2 extends Aggregator<User, AgeBuffer, Long> {

    // 缓冲区初始值
    @Override
    public AgeBuffer zero() {
        return new AgeBuffer(0L, 0L);
    }

    // 缓冲区数值更新
    @Override
    public AgeBuffer reduce(AgeBuffer buffer, User user) {
        buffer.setTotalAge(buffer.getTotalAge() + user.getAge());
        buffer.setCount(buffer.getCount() + 1);
        return buffer;
    }

    // 缓冲区是分布式的，有多个的时候需要合并。
    @Override
    public AgeBuffer merge(AgeBuffer buffer1, AgeBuffer buffer2) {
        buffer1.setTotalAge(buffer1.getTotalAge() + buffer2.getTotalAge());
        buffer1.setCount(buffer1.getCount() + buffer2.getCount());
        return buffer1;
    }

    // 缓冲区结果的计算
    @Override
    public Long finish(AgeBuffer reduction) {
        return reduction.getTotalAge() / reduction.getCount();
    }

    // 指定缓冲区类型编码
    @Override
    public Encoder<AgeBuffer> bufferEncoder() {
        return Encoders.bean(AgeBuffer.class);
    }

    // 指定返回值类型编码
    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}
