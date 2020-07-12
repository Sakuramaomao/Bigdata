package com.lzj.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 转换算子。
 *
 * 将旧RDD -> 算子 -> 新RDD
 *
 * collect算子不会转换RDD，会触发作业执行，所以将collect这样的方法称之为行动(Action)算子。
 *
 * @Author Sakura
 * @Date 2020/7/12 23:14
 */
public class Spark06_RDDOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("file-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 元素乘以2
        JavaRDD<Integer> resultRdd = rdd.map(i -> i * 2);

        // 计算
        List<Integer> collect = resultRdd.collect();
        System.out.println(collect.toString());

        sc.stop();
    }
}
