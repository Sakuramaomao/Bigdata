package com.lzj.spark.core.rdd.operator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.runtime.ScalaWholeNumberProxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <pre>
 *   partitionBy算子中自定义分区器。
 *
 *   Java的写法中，需要继承Partitioner抽象类，实现其中的以下两个方法。（如果想在使用时自定义分区数量，可以自己加个有参构造器。）
 *      （1）numPartitions：指定分区数量。
 *      （2）int getPartition(Object key)：自己编写逻辑，根据key来决定将key分配到那个分区中，返回分区号。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/17 16:56
 **/
public class Spark19_RDDCustomPartitionerOperator {
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

        // 创建K-V结构RDD。
        JavaPairRDD<String, String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2("nba", "aa"),
                new Tuple2("cba", "bb"),
                new Tuple2("nba", "cc"),
                new Tuple2("dba", "tt")
        ));

        JavaPairRDD<String, String> newRdd = rdd.partitionBy(new MyPartitioner(2));

        JavaRDD<Tuple2<Integer, Tuple2<String, String>>> rdd2 = newRdd.mapPartitionsWithIndex((Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Tuple2<Integer, Tuple2<String, String>>>>) (v1, v2) -> {
            ArrayList<Tuple2<Integer, Tuple2<String, String>>> list = new ArrayList<>();
            while (v2.hasNext()) {
                Tuple2<String, String> next = v2.next();
                list.add(new Tuple2<>(v1, next));
            }
            return list.iterator();
        }, false);

        List<Tuple2<Integer, Tuple2<String, String>>> collect = rdd2.collect();
        System.out.println(collect.toString());
    }
}

class MyPartitioner extends Partitioner {
    private final int partitions;

    MyPartitioner(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {

        if ("nba".equals(key.toString())) {
            return 0;
        }
        return 1;
    }
}
