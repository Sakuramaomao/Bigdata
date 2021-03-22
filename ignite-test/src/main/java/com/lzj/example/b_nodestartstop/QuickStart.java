package com.lzj.example.b_nodestartstop;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.util.Collections;

/**
 * <pre>
 *
 *  * 节点启停
 *    （1）Ignite实现了AutoCloseable接口，可以使用try-resource自动关闭。
 *    （2）所有节点默认都是以服务端模式启动，客户端模式需要手动指定。
 *    （3）不能强制停止节点，可能导致数据丢失或者数据不一致。
 *        a、正确方式是使用Ignite.close()或者System.exit()，或者Ctrl + c发送中断信号。
 *        b、从基线拓扑中（baseline topology）删除某个节点会触发其他节点的数据平衡，如果只是重启某个节点，请不要将节点从基线拓扑中移除。正常重启即可。
 *
 *  * 节点启停过程的生命周期事件
 *     可以在生命周期的不同阶段执行自定义代码。阶段一共有四个，分别如下：
 *    （1）BEFORE_NODE_START: Ignite节点的启动程序执行前。
 *    （2）AFTER_NODE_START: Ignite节点启动完成后调用。
 *    （3）BEFORE_NODE_STOP: Ignite节点的停止程序执行前。
 *    （4）AFTER_NODE_STOP: Ignite节点停止后调用。
 *     借助${@link LifecycleBean}来实现注册节点生命周期事件。
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

        // 注册自定义的生命周期事件。
        cfg.setLifecycleBeans(new MyLifeCycleTest());

        // 设置网络节点自动发现的SPI
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // 启动节点。
        Ignite ignite = Ignition.start(cfg);

        ignite.close();
    }

    public static class MyLifeCycleTest implements LifecycleBean {

        @Override
        public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
            if (evt == LifecycleEventType.BEFORE_NODE_START) {
                System.out.println("====BEFORE_NODE_START====；节点准备启动了！");
            }
            if (evt == LifecycleEventType.AFTER_NODE_START) {
                System.out.println("====AFTER_NODE_START====：节点启动完成了！");
            }
            if (evt == LifecycleEventType.BEFORE_NODE_STOP) {
                System.out.println("====BEFORE_NODE_STOP====：节点准备停止了！");
            }
            if (evt == LifecycleEventType.AFTER_NODE_STOP) {
                System.out.println("====AFTER_NODE_STOP====：节点已经停止了！");
            }
        }
    }

}
