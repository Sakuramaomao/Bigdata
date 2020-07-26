package com.lzj.spark.core.rdd.serial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

/**
 * <pre>
 *     序列化问题。
 *       从计算的角度考虑，算子之外的代码都是在Driver端执行；算子之内的方法都是在Executor
 *     内执行。
 *       比如下面的例子，需要用到样板例对象。当样板例对象的创建被抽取到算子外部创建时，
 *     创建对象的代码是在Driver端进行的，而算子中逻辑执行是在Executor中，使用到的对象
 *     需要在网络中进行传输，就要求序列化。
 *
 *     注意：
 *      1、当使用到样板例对象时，要记住进行序列化。
 *      2、序列化的原因是闭包。在runJob之前的clean方法中会检测算子逻辑是否能通过序列化检测器。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 14:32
 */
public class Spark01_Serialize {
    public static void main(String[] args) {
         /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("serialize");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        User user = new User();

        rdd.foreach(i -> {
            System.out.println(user.age + i);
        });
    }

}

class User implements Serializable {
    int age = 20;
}
