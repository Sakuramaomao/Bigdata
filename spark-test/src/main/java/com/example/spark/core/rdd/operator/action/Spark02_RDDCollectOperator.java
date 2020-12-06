package com.example.spark.core.rdd.operator.action;

/**
 * <pre>
 *     Collect 行动算子
 *        将所有分区的计算结果拉取到当前节点的内存中，可能会出现内存溢出。
 *     平时测试的时候可以随便使用。但是如果需要在生产中使用，需要三思，如果
 *     数据量很大，没有聚合操作，会OOM。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 10:09
 */
public class Spark02_RDDCollectOperator {
    public static void main(String[] args) {
        //  太过于简单，这里就不练习了。
    }
}
