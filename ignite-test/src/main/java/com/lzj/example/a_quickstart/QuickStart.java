package com.lzj.example.a_quickstart;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.springframework.context.ApplicationContext;

import java.util.Collections;

/**
 * <pre>
 * 其中，Ignition是个工厂类，提供方便的。Ignite才是提供的Ignite API入口。
 *
 *  * 可以通过Ignition.ignite()来获取Ignite实例。同一个JVM进程中可以存在多个Ignite实例。
 *   也可以给不同的Ignite取不同的名字来区分，并且通过名字来获取对应的Ignite实例。
 *
 *  * 配置方式
 *    Ignite节点的配置有两种。一种是基于Spring XML，另一种是编程方式。两种方式的共同点：都会
 *    使用IgniteConfiguration配置和类来进行配置。
 *
 *  * 启用模块
 *    Ignite二进制安装包中包含了所有的可选模块。
 *    已经启用的模块如下
 *    （1）Ignite-Spring
 *    （2）Ignite-Core
 *    （3）Ignite-Indexing：SQL查询和索引。
 *    （4）Ignite-Kafka：提供了从Kafka到Ignite缓存的流式数据处理能力。
 *    （5）Ignite-ssh：提供了通过SSH在远程主机上启动Ignite节点的功能。
 *     ......
 *
 *  * 资源注入
 *    （1）如果Ignite实例已经建立，就可以通过${@link IgniteInstanceResource}属性注入或者方法注入的方式直接使用了。
 *    （2）通过${@link SpringApplicationContextResource}属性注入或者方法注入的方式直接使用${@link ApplicationContext}
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/18 21:45
 */
public class QuickStart {
    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        // 以客户端模式运行。
        cfg.setClientMode(true);
        // 所需要的类会通过网络传输到集群。即对等类加载。
        cfg.setPeerClassLoadingEnabled(true);

        // 设置网络节点自动发现的SPI
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // 启动节点。
        Ignite ignite = Ignition.start(cfg);

        // 功能1：创建缓存
        IgniteCache<String, Object> cache = ignite.getOrCreateCache("myCache");
        cache.put("key1", "hello");
        cache.put("key2", "world");

        // 功能2：在集群中执行普通Java Compute Task。
        ignite.compute(ignite.cluster().forServers()).broadcast(new myRemoteTask());

        ignite.compute(ignite.cluster().forServers()).broadcast(new IgniteRunnable() {
            // 通过属性注入Ignite实例。
            @IgniteInstanceResource
            private Ignite ignite2;

            @Override
            public void run() {
                System.out.println(ignite2.cluster().localNode().id());
            }
        });

        ignite.close();
    }

    /**
     * 自定义Java Compute Task。可以提交到集群运行。
     */
    public static class myRemoteTask implements IgniteRunnable {
        // 可以很方便的注入当前JVM中的ignite实例。
        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public void run() {
            System.out.println(">> executing now!!");
            System.out.println("Node Id:" + ignite.cluster().localNode().id());

            IgniteCache<String, Object> myCache = ignite.cache("myCache");
            String value1 = myCache.get("key1").toString();
            String value2 = myCache.get("key2").toString();
            System.out.println(">> " + value1 + " " + value2);
        }
    }
}
