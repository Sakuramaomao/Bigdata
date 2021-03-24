package com.lzj.example.e_datamodel;

/**
 * <pre>
 *   关联函数
 *    （1）关联函数控制这缓存和分区、分区和节点之间的映射。默认实现了约会哈希算法。
 *    （2）分区到节点之间的映射可以有略微差异。即某些节点分区多一些。
 *    （3）关联函数可以保证当集群拓扑更改时，分区仅仅迁移到新节点或者从离开的节点迁移，
 *    其余节点没有数据交换。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2021/3/24 19:46
 **/
public class d_AffinityFunction {
}
