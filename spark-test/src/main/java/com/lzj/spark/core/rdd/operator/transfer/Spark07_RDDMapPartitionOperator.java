package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * <pre>
 * mapPartitions
 * 以分区为单位进行计算，和map算子很像。
 * 区别：
 *  1、map算子是一个一个执行，而mapPartitions一个分区一个分区的执行。类似于批处理
 *
 *  2、map方法是全量数据操作，不能丢失数据。而mapPartitions是一次性获取分区的所有数据，
 *      那么可以执行迭代器集合的所有操作，比如过滤、max和min等。
 *
 * 对于Map和MapPartitions的选择：
 *    有一个特性是决定选择map还是mapPartitions的，这个特性就是mapPartitions一次处理的是一个分区的数据，
 * 只有在全部处理完成后，才会释放所有的数据。当机器内存不够用时，容易发生OOM。所以，依据这一点，可以知道，当内存
 * 比较充裕时，可以使用mapPartitions来加快速度。当内存不太足时，需要使用map来保证程序执行的通。
 *    有些时候，完成比完美更重要。
 * </pre>
 */
public class Spark07_RDDMapPartitionOperator {
    public static void main(String[] args) {
         /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("mappartitions-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3);
        rdd.saveAsTextFile("output");

        JavaRDD<Integer> mapPartitionsRdd = rdd.mapPartitions((iter -> {
            // 对每个分区进行操作。相当于批处理。所以传入的是迭代器，方便对元素的遍历。
            // 练习1、取偶数
            //ArrayList<Integer> result = new ArrayList<>();
            //while (iter.hasNext()) {
            //    Integer next = iter.next();
            //    if (next % 2 == 0) {
            //        result.add(next);
            //    }
            //}
            //return result.iterator();
            // 练习2、取分区中最大值
            int max = iter.next();
            while (iter.hasNext()) {
                Integer next = iter.next();
                if (next > max) {
                    max = next;
                }
            }
            return Collections.singletonList(max).iterator();
        }));

        List<Integer> collect = mapPartitionsRdd.collect();
        System.out.println(collect);
    }
}
