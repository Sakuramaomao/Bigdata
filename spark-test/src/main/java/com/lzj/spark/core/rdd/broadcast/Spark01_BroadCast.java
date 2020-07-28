package com.lzj.spark.core.rdd.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *     广播变量
 *      1、一个场景
 *          join操作会有笛卡尔乘积效果，数据量会急剧增多。如果有shuffle操作，
 *      那么性能会非常低。为了实现同样的效果而不用shuffle操作，可以将数据独立出来，
 *      防止shuffle操作。虽然数据独立出来了，但是会导致同一个Executor中的多个Task
 *      都会复制一份，那么在Executor内存中会有大量的冗余数据，性能不高，所以可以采用
 *      广播变量，将数据只保存一份在Executor内存中。
 *      2、广播变量含义
 *          分布式共享只读变量。创建后Executor无法改变广播变量，只能访问它。
 *      3、小练习
 *          将join操作使用广播变量方式实现（非shuffle）。
 *
 *          join结果：[(a,(1,2)), (b,(2,4)), (c,(4,5))]。
 *          实现目标：采用广播变量，和上面结果一致。
 *      4、注意
 *          广播变量也不是算子，是和RDD同级别的数据模型。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/28 21:59
 */
public class Spark01_BroadCast {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("partitionBy-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkContext);

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("c", 4)
        ));

        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 4),
                new Tuple2<>("c", 5)
        ));
        List<Tuple2<String, Integer>> tups = Arrays.asList(
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 4),
                new Tuple2<>("c", 5)
        );

        // join有shuffle操作。
        // JavaPairRDD<String, Tuple2<Integer, Integer>> joinRdd = rdd1.join(rdd2);

        // 创建广播变量。
        Broadcast<List<Tuple2<String, Integer>>> bc = sc.broadcast(tups);

        JavaRDD<Tuple2<String, Tuple2<Integer, Integer>>> mapRdd = rdd1.map(new Function<Tuple2<String, Integer>, Tuple2<String, Tuple2<Integer, Integer>>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Integer> v1) throws Exception {
                Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2 = null;
                // 使用广播变量
                for (Tuple2<String, Integer> t : bc.value()) {
                    if (t._1.equals(v1._1)) {
                        stringTuple2Tuple2 = new Tuple2<>(v1._1, new Tuple2<>(v1._2, t._2));
                    }
                }
                return stringTuple2Tuple2;
            }
        });

        // 结果[(a,(1,2)), (b,(2,4)), (c,(4,5))]
        //System.out.println(joinRdd.collect());

        // 结果[(a,(1,2)), (b,(2,4)), (c,(4,5))] 和使用shuffle的join结果一致。
        System.out.println(mapRdd.collect());
    }
}
