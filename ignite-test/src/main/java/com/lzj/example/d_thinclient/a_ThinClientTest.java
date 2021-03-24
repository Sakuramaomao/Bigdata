package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * <pre>
 *     集群配置可以接受客户端请求。
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/22 20:44
 */
public class a_ThinClientTest {
    public static void main(String[] args) {
        // 如果当前应用是集群模式，就可以配置ClientConnectorConfiguration控制暴露给thinClient的端口了。
        ClientConnectorConfiguration cfg = new ClientConnectorConfiguration();
        // set a port range from 10800 to 10805;
        cfg.setPort(10800);
        cfg.setPortRange(5);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setClientConnectorConfiguration(cfg);

        Ignite ignite = Ignition.start();

    }

}
