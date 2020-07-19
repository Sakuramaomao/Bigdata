package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *    groupByKey算子。
 *
 *    根据指定的key进行分组，相同key对应的是个迭代器集合。
 *    和groupBy的区别在于groupBy可以自定义分组规则，而groupByKey只能依据key来分组。
 *
 *    其实groupByKey拿到可迭代器，是可以再进一步做聚合操作的。这也是reduceByKey比它多的一个小功能。
 *
 *    关于groupByKey原理的细节：
 *      我们知道groupByKey会按照key对所有分区中的数据进行分组（面向整个数据集，而非单独一个分区），其实现
 *    的过程中对一个分区中数据分组后不能继续执行后序的操作，比如统计之类的计算。需要等待其他分区的数据全部到
 *    达后，才能执行后续计算。
 *       但是这个等待操作如果在内存中进行，等待时间过长，可能会引发OOM，所以这个等待的过程产生的数据应该被
 *    存储到磁盘中。即shuffle过程中的数据是要落盘的，当数据准备完全后，会被重新加载到内存中进行后续的计算
 *    任务。
 *       有shuffle过程的算子，无法进行后续计算。因为需要等待数据重组，所以在shuffle前后会被分为两个Task来执行。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/17 19:54
 **/
public class Spark21_RDDGroupByKeyOperator {
    public static void main(String[] args) {
/*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("myPartitioner-rdd");

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

        // 返回的元组中第一个元素是key。
        // 返回的元组中第二个元素是根据key分组后，相同key的value集合。
        JavaPairRDD<String, Iterable<Integer>> rdd1 = rdd.groupByKey();

        System.out.println(rdd1.collect());
    }
}
