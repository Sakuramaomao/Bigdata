package com.example.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * <pre>
 *     本小节开启新的算子篇章。
 *     Spark算子 - 行动
 *        所谓的行动算子，其实不会再产生新的RDD，而是触发作业的执行。
 *        所谓的转换算子，不会触发作业的执行，只是功能的扩展和包装。
 * -------------------------------------------------------
 *     reduce行动算子
 *       聚集RDD中的所有数据。先聚合分区内数据，再聚合分区间数据。
 *
 *     和reduceByKey的区别？
 *        reduce是行动算子，会聚集RDD中所有数据。其返回结果是一个值。而reduceByKey是转换算子，
 *     根据key进行分区并在分区内预聚合，分区间再聚合。其返回结果还是一个RDD。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 22:46
 */
public class Spark01_RDDReduceOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("reduce-action");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        Integer reduceRdd = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(reduceRdd);
    }
}
