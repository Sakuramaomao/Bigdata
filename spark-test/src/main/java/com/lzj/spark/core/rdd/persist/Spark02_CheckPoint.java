package com.lzj.spark.core.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     检查点（CheckPoint）
 *          将RDD中的数据保存到分布式文件系统中。
 *
 *     1、为何设置了checkPoint，还是会重头开始计算？
 *          因为保存到检查点的数据一般都是比较重要的数据，所以检查点的为了保证数据的正确性，在执行
 *       时，会启动新的job重新计算一遍之前的数据。
 *          为了提高效率，防止重新计算一次之前的数据，checkPoint一般是和cache联合使用的。
 *
 *
 *     2、检查点和血缘关系？
 *          检查点会切断血缘关系。一旦数据丢失，前面的数据将无法重头计算得到了。
 *       这么做也是对的。因为检查点会将数据保存在分布式文件系统中，比如HDFS。数据相对来说比较安全，
 *       数据不容易丢失。如果连HDFS都挂了，那么生态环境也就出问题了。所以会切断血缘关系，这样等同于
 *       产生新的数据源了。
 *
 *        (4) ParallelCollectionRDD[0] at parallelize at Spark02_CheckPoint.java:40 []
 *
 *        (4) ParallelCollectionRDD[0] at parallelize at Spark02_CheckPoint.java:40 []
 *          |  ReliableCheckpointRDD[1] at collect at Spark02_CheckPoint.java:46 []
 *
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 23:11
 */
public class Spark02_CheckPoint {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("checkPoint");


        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // 设置检查点
        sc.setCheckpointDir("cp");

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaPairRDD<Integer, Integer> mapRdd = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                System.out.println("----map----");
                return new Tuple2<>(integer, 1);
            }
        });

        // 为了不让检查点的数据重新计算一次，checkPoint一般会配合cache一起使用。
        mapRdd.cache();

        // 检查血缘关系
        System.out.println(mapRdd.toDebugString());
        mapRdd.checkpoint();

        System.out.println(mapRdd.collect());
        System.out.println(mapRdd.toDebugString());
    }
}
