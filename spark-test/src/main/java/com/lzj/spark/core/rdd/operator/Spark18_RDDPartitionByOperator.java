package com.lzj.spark.core.rdd.operator;

import org.apache.spark.HashPartitioner;
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
 *     分区器：
 *      （1）HashPartitioner   根据key的hash值分区。（这里没有用跟map一样的位运算，因为分区数是手动填写的，不能保证是2的n次幂。）
 *      （2）RangePartitioner  直接划分范围进行分区。比如0-100到零号分区，101-200到一号分区。
 *     默认分区器是HashPartitioner。RangePartitioner要求key必须是可以排序的，这个在sortBy算子中被默认使用了，平时不会去使用。
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

        // 内置分区器有Hash和Range两种，实现和Kafka中的分区机制类似。
        JavaPairRDD<String, Integer> rddPartitionByKey = rdd.partitionBy(new HashPartitioner(2));

        rddPartitionByKey.saveAsTextFile("output");
    }
}
