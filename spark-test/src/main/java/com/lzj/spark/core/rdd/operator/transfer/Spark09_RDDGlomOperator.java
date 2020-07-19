package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * glom操作。
 * 直接将分区中的数据变为一个数组(Java中是List集合)。
 *
 * @Author Sakura
 * @Date 2020/7/13 23:02
 */
public class Spark09_RDDGlomOperator {
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

        // 此时分区中数据都是一个个Integer元素。
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);

        // 将分区中Integer元素通过glom算子转存到List集合。
        JavaRDD<List<Integer>> glomRdd = rdd.glom();

        // 小练习：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        glomRdd.mapPartitions(iter -> {
            List<Integer> parList = iter.next();
            // FIXME Comparator写的有问题。得出的不是最大值。
            Integer max = parList.stream().max(Comparator.comparingInt(v -> v)).get();
            return Arrays.asList(max).iterator();
        });

        List<List<Integer>> collect = glomRdd.collect();

        int sum = 0;
        for (List<Integer> list : collect) {
            sum+=list.get(0);
        }
        System.out.println(sum);
    }
}
