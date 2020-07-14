package com.lzj.spark.core.rdd.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * <pre>
 *  内存创建RDD的分区数量（并行度）。
 *
 *  第二个参数是设置并行度，即分区数量。
 *                      parallelize
 *  如果不设置，则默认值为<b>当前环境中可用的核数</b>：
 *      1、当在Standalone模式cluster模式提交时默认值: conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
 *  即：SparkConf有配置值，就用配置值；没有配置，使用totalCore和2比较的最大值作为并行度。
 *      2、当在服务器上提交时默认值：scheduler.conf.getInt("spark.default.parallelism", totalCores)
 *  即：SparkConf有配置值，就用配置值；没有配置，就用totalCores值。
 *
 *  注意：totalCore = 当前环境中可用的核数。
 *      假如不设置，核树则根据设置的环境有关。以开发环境为例。
 *      local时，默认单核，并行度为1.
 *      local[n]，指定n个核，并行度为n。
 *      local[*]，机器核心数量。并行度为机器的核心数量。
 *
 * API：saveAsTextFile：将每个分区的数据写入到文件中。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/12 16:41
 */
public class Spark03_RDDInMemPartition {
    public static void main(String[] args) {
         /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        // 并行度为所有核心数量。
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("file-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 第二个参数是设置并行度，即分区数量。
        //                      parallelize
        // 当在Standalone模式cluster模式提交时默认值: conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
        // 当在服务器上提交时默认值：scheduler.conf.getInt("spark.default.parallelism", totalCores)
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        // local环境，而且是IDEA，所以会自动写出到项目根目录下的output文件夹中。
        // 将RDD处理后的数据保存到分区文件中。会按照分区的方式来保存分区文件。
        rdd.saveAsTextFile("output");
    }
}
