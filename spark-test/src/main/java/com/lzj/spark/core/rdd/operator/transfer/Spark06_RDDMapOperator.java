package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * 转换算子。
 *
 * 1、map的作用
 * 将旧RDD -> 算子 -> 新RDD
 *
 * 注意：collect算子不会转换RDD，会触发作业执行，所以将collect这样的方法称之为行动(Action)算子。
 *
 * 2、当有多个map时，对元素的执行顺序。
 *    当有多个map级联时，有以下两点需要注意。
 *    （1）分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据。
 *    （2）分区间数据执行是没有顺序的，而且无需等待，即分区之间都是独立的。
 *     举个例子：
 *      1,2,3,4,5数组，指定了两个分区。
 *
 *      分区  数据      处理顺序
 *      0 -> (1,2)    1A(2B) 2A(4B)
 *      1 -> (3,4,5)                 3A(6B)  4A(8B)  5A(10B)
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/12 23:14
 */
public class Spark06_RDDMapOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("map-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // 元素乘以2
        JavaRDD<Integer> resultRdd = rdd.map(i -> {
            System.out.println("map A :" + i);
            return i * 2;
        });

        JavaRDD<Integer> resultRdd2 = resultRdd.map(i -> {
            System.out.println("map B :" + i);
            return i * 2;
        });

        // 计算
        List<Integer> collect = resultRdd2.collect();
        System.out.println(collect.toString());

        sc.stop();
    }
}
