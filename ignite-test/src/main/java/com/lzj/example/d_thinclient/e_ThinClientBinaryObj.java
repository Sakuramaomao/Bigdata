package com.lzj.example.d_thinclient;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * <pre>
 *     瘦客户端处理二进制对象
 *
 *     可以在客户端直接创建二进制对象，然后缓存起来。
 *
 *     对于二进制对象的处理不需要序列化和反序列化。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/23 21:59
 */
public class e_ThinClientBinaryObj {
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("localhost:10800");

        IgniteClient client = Ignition.startClient(cfg);

        IgniteBinary binary = client.binary();
        // 创建二进制对象。
        BinaryObject person = binary.builder("Person")
                .setField("id", 1, Integer.class)
                .setField("name", "lzj_1", String.class)
                .build();

        // 将二进制对象缓存起来。
        ClientCache<Integer, BinaryObject> persons = client.getOrCreateCache("persons").withKeepBinary();
        persons.put(1, person);

        BinaryObject value = persons.get(1);
    }
}
