package com.example.spark.core.rdd.dep;

/**
 * <pre>
 *     Spark依赖关系之宽窄依赖。
 *
 *      窄依赖（独生）
 *          父RDD中的每一个分区最多被子RDD中的一个分区所使用，这种依赖称之为窄依赖（narrowDependency）
 *        比如map、coalesce（coalesce合并分区过程中无shuffle）等操作，是窄依赖。
 *
 *      宽依赖（多生）
 *          父RDD中的一个分区被子RDD中的多个分区所使用，会引起shuffle操作，这种关系称之为宽依赖（shuffleDependency）。
 *        比如Shuffle操作，一定是宽依赖。
 *
 *      注意：
 *          1、依赖只有宽依赖和窄依赖。除了宽就是窄。
 *          2、shuffle操作一定是宽，除了shuffle操作，其余是窄。
 *
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 16:00
 */
public class Spark02_DependenceType {
    public static void main(String[] args) {
        // scala中有dependency方法，但是java中没有。
    }
}
