package com.lzj.spark.core.rdd.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     partitionBy算子。只适用于K-V类型（对偶元组）的RDD，普通单值类型的RDD无法调用此方法。
 *
 *     注意：partitionBy算子不是RDD接口中的方法，而是PairRDD接口中的方法。
 *     所以有些时候ide不提示方法，是因为不是当前对象中所拥有的方法。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/16 23:03
 */
public class Spark18_RDDPartitionByOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("partitionBy-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // partitionBy算子不是RDD接口中的方法，而是PairRDD接口中的方法。
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("b", 2),
                        new Tuple2<>("a", 4)
                )
        );

        //
        JavaPairRDD<String, Integer> rddPartitionByKey = rdd.partitionBy(null);

        System.out.println(rddPartitionByKey.collect());
    }
}
