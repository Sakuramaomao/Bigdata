package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * <pre>
 *  Map和MapPartitionsWithIndex。
 *
 *  mapPartitions和mapPartitionsWithIndex的区别：
 *      前者只有分区，后者可以获得当前分区的分区号。比如可以获取指定分区号的数据（注意：分区号从0开始）。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/13 22:00
 */
public class Spark08_RDDMapPartitionOperator2 {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("mappartitionsIndex-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 3);

        JavaRDD<Integer> rdd1 = rdd.mapPartitionsWithIndex((Function2<Integer, Iterator<Integer>, Iterator<Integer>>) (partitionIndex, iter) -> {
            // 练习1：求分区最大值并输出分区号。
            //// 求分区最大值。
            //int max = iter.next();
            //while (iter.hasNext()) {
            //    Integer next = iter.next();
            //    if (next > max) {
            //        max = next;
            //    }
            //}
            //// 输出分区号。
            //List<Tuple2<Integer, Integer>> tuple2s = Arrays.asList(new Tuple2<>(partitionIndex, max));
            //return tuple2s.iterator();

            // 练习2：只输出第二个分区的数据。
            if (partitionIndex == 1) {
                return iter;
            } else {
                return Collections.EMPTY_LIST.iterator();
            }
        }, false);

        List<Integer> collect = rdd1.collect();
        System.out.println(collect);
    }
}
