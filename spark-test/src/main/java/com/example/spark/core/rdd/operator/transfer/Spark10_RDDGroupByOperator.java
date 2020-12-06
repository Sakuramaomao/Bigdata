package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *  groupBy算子：
 *
 *  groupBy 可以根据指定的规则进行分租，指定规则的返回值就是分组的key。
 *  groupBy 返回值是二元组。
 *      1、第一个元素表示分组的key。
 *      2、第二个元素表示相同key的数据形成的可迭代集合。
 *
 *  注意：
 *      1、groupBy操作不会改变RDD的分区，新旧RDD的分区是一致的。
 *      2、groupBy操作会将旧RDD分区中的数据打乱在新RDD的分区中。
 *          可能会造成分区数据不均匀，比如下面例子中就会导致第三个分区中没有数据，而前面两个分区有三个数据。
 *       <b>这种上游分区数据打乱再重新组合到下游分区的操作，称之为shuffle。</b>
 *       所以groupBy操作会导致分区数据不均匀，从而产生shuffle。在生产环境中可能会出现<b>数据倾斜</b>。
 *       如果想改变不均匀，可以通过groupBy中的partitions参数来重新平衡数据。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/14 20:04
 **/
public class Spark10_RDDGroupByOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("groupBy-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);

        // 分组练习（分区数据打乱重组后不均匀）
        //JavaPairRDD<Integer, Iterable<Integer>> rdd1 = rdd.groupBy((Function<Integer, Integer>) v1 -> v1 % 2);

        // 分组后重新分区。(这里是依据组的数量来分区就比较均衡了，不会出现未使用的分区。)
        JavaPairRDD<Integer, Iterable<Integer>> rdd1 = rdd.groupBy((Function<Integer, Integer>) v1 -> v1 % 2, 2);
        System.out.println(rdd1.glom().collect().size());
        rdd1.saveAsTextFile("output");

        List<Tuple2<Integer, Iterable<Integer>>> collect = rdd1.collect();

        System.out.println(collect.toString());

        sc.stop();
    }
}
