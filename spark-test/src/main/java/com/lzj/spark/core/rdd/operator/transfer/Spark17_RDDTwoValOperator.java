package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * <pre>
 *     双Value操作算子。即对两个RDD进行操作的算子。
 *     
 *     对两个RDD可以求并集、交集、差集与拉链。
 *     
 * </pre>
 * 
 * @Author Sakura
 * @Date 2020/7/16 22:14
 */
public class Spark17_RDDTwoValOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("twoval-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));
        
        // 并集
        // 特点：数据合并，分区也会合并。
        //      两个RDD类型必须一致。
        JavaRDD<Integer> unionRdd = rdd1.union(rdd2);
        
        // 交集
        // 特点：保留最大分区数，数据被打算重组，shuffle。
        //      两个RDD类型必须一致。
        JavaRDD<Integer> interRdd = rdd1.intersection(rdd2);

        // 差集
        // 特点：数据被打乱重组，shuffle。
        // 分区会以驱动RDD的分区数为主。
        //      两个RDD类型必须一致。
        JavaRDD<Integer> subRdd = rdd1.subtract(rdd2);

        // 拉链
        // 特点：分区需要一致，每个分区中的数据量也要求一致。
        //      因为返回的是Tuple，所以两个RDD的类型可以不一致。
        JavaPairRDD<Integer, Integer> zipRdd = rdd1.zip(rdd2);

        System.out.println(unionRdd.collect());
        System.out.println(interRdd.collect());
        System.out.println(subRdd.collect());
        System.out.println(zipRdd.collect());
    }
}
