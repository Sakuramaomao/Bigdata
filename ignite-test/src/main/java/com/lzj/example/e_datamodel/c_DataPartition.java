package com.lzj.example.e_datamodel;

/**
 * <pre>
 *  数据分区：将大型数据集细分为较小的块，然后在所有服务端节点之间平均分配的方法。
 *
 *  * 关联函数
 *    分区由关联函数控制（Affinity Function）。关联函数确定键和分区之间的映射。
 *      KV cache -> AF -> Partitions -> AF -> 节点
 *
 *  * 关联键
 *    关联函数将关联键作为参数。关联键可以是存储对象中的任何字段（SQL表）。如果未指定，
 *    KV中默认使用K作为关联键，SQL表中默认使用主键。
 *
 *  * 关联并置
 *    可以使同一类数据条目存储在一个分区中。当请求该数据时，只需要扫描少了分区，这种技术
 *    称为关联并置。
 *      比如Person和Company对象，每个人都有一个companyId字段。通过将Person.companyId和
 *    Company.companyId作为关联键，就可以保证同一公司的所有人都存储在一个节点上，该节点也
 *    存储了公司对象。这就叫做数据关联并置。
 *      除此之外，还可以将计算任务和数据做关联并置。
 *
 *  * 线性可伸缩
 *     随着集群中节点的增多，Ignite会保证分区数据在节点之间的平衡（平均分布）。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2021/3/24 19:31
 **/
public class c_DataPartition {
}
