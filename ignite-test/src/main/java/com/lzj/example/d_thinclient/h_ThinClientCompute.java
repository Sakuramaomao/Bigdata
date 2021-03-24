package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * <pre>
 *   瘦客户端执行计算任务。
 *
 *   瘦客户端通过执行集群中已经部署的计算任务来支持基本的计算功能。
 *   要求是必须将计算任务打成一个Jar文件，放到集群节点的类路径中。
 *   然后使用Ignite实例的compute().execute()方法来指定计算任务的全限定名，开始执行。
 *
 *   上述这种计算任务在集群中是默认关闭的，需要在集群节点设置
 *   ThinClientConfiguration.maxActiveComputeTasksPerConnection参数设置为非零值才能启用。
 * </pre>
 *
 * @Author zj.li
 * @Date 2021/3/24 18:12
 **/
public class h_ThinClientCompute {
    public static void main(String[] args) {
        final ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("127.0.0.1:10800");

        try (IgniteClient client = Ignition.startClient(cfg)) {
            client.compute().execute("com.lzj.MyTask", "args");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
