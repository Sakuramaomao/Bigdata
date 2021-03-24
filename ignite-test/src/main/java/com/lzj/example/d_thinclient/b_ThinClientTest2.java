package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 *
 * 接入集群。
 * 瘦客户端，用来向集群发送请求。
 *
 * @Author Sakura
 * @Date 2021/03/22 21:18
 */
public class b_ThinClientTest2 {
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration();
        // 可以填写多个。瘦客户端在启动时会自动选择一个连接。如果连接的节点挂掉了，就会选择地址列表中的一个其他节点连接。
        cfg.setAddresses("localhost:10800");

        try(IgniteClient client = Ignition.startClient(cfg)) {
            ClientCache<Object, Object> cache = client.getOrCreateCache("myCache");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
