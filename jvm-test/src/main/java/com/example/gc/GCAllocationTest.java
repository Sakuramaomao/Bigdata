package com.example.gc;

import java.io.IOException;

/**
 * <pre>
 *  -verbose:gc
 *  -Xms20m
 *  -Xmx20m
 *  -Xmn10m
 *  -XX:SurvivorRatio=8
 *  -XX:+PrintGCDetails
 *
 *  堆内存20m，不允许动态扩展。
 *  新生代10m，剩余10m给老年代。
 *  新生代中Eden和Survivor比例8:1。
 *  新生代可用容量 Eden + Survivor = 8（8192K） + 1（1024K） = 9m（9216K）
 *
 *  实验结果和书中不一致。发生Minor GC时，新生代不会全部都进入老年代，只会进入两个（共4MB），bytes4还是在Eden区域分配的。
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/02 20:11
 */
public class GCAllocationTest {
    private static final int _1MB = 1024 * 1024;

    public static void main(String[] args) throws IOException {
        byte[] bytes1 = new byte[2 * _1MB];
        byte[] bytes2 = new byte[2 * _1MB];
        byte[] bytes3 = new byte[2 * _1MB];
        byte[] bytes4 = new byte[4 * _1MB]; // 发生一次Minor GC
        //System.in.read();
    }
}
