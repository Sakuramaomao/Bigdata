package com.lzj.spark.core.rdd.dep;

/**
 * <pre>
 *     Spark中的stage划分
 *        如果没有落盘操作，那么计算可以是一个整体，一气呵成。但是因为shuffle操作
 *     的存在，会导致多个阶段的产生。
 *        根据shuffle算子来进行阶段的划分。shuffle算子的前后会分别产生一个stage。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 16:44
 */
public class Spark03_StageDivide {
    public static void main(String[] args) {
        // 没有练习
    }
}
