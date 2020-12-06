package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *    reduceByKey算子。
 *
 *    根据数据的key进行分组，然后对value进行聚合。
 *
 *    有可能在聚合后造成分区数据不均匀的情况，所以第二个参数可以修改分区大小。
 *
 *    关于reduceByKey算子的原理细节：
 *      我们都知道reduceByKey算子实现的是针对所有数据集，而非单个分区依据key进行分区聚合的功能。
 *    reduceByKey会在shuffle前，先对每个分区的数据做一个预聚合，可以有效减少对单个分区数据分组
 *    后落盘的数据量，即shuffle过程中数据量减少，可以提高性能。当shuffle过程结束，数据准备完毕，
 *    重新装载到内存后，再执行一次聚合，得到和【groupByKey + 聚合】一致的结果。
 *
 *    reduceByKey和groupByKey的区别：
 *      当场景为分组后聚合时，reduceByKey和groupByKey的结果是一致的。但是从效率上来说reduceByKey
 *    更高一些。虽然两者都要shuffle操作，我们也都知道shuffle会落盘，但是由于reduceByKey会在旧RDD中
 *    做预聚合操作，可以减少落盘的数据量。而groupByKey则是将分区中的相同key数据直接落盘，当数据量大时，
 *    IO慢，总体速度就慢。
 *      所以，当场景中需要分组后聚合的操作时，推荐优先选用reduceByKey算子。如果场景中不需要聚合，那可以
 *    选用groupByKey算子。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/17 19:43
 **/
public class Spark20_RDDReduceByKeyOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("groupByKey-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("key1", 1),
                new Tuple2<>("key2", 1),
                new Tuple2<>("key1", 1),
                new Tuple2<>("key2", 1),
                new Tuple2<>("key1", 1)
        ));

        JavaPairRDD<String, Integer> rdd2 = rdd.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        List<Tuple2<String, Integer>> collect = rdd2.collect();

        System.out.println(collect.toString());

    }
}
