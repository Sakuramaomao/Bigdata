package com.example.spark.core.rdd.dep;

/**
 * <pre>
 *     Spark中的Task的划分。
 *        Application：初始化一个SparkContext即生成一个Application。
 *        Job：一个Action算子就会生成一个Job。（并非绝对，比如sortBy，转换算子，但是也会提交Job。
 *            如果转换算子中含有行动操作，那么转换算子也就隐式的提交了一个Job。不建议在转换中使用行动算子）
 *        Stage：Stage等于宽依赖（ShuffleDependency）个数加1.
 *        Task：一个Stage中，最后一个RDD的分区数量就是Task的个数。<b>即一个分区对应一个Task</b>。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 17:15
 */
public class Spark04_TaskDivide {
}
