package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import javax.cache.Cache;

/**
 * @Author Sakura
 * @Date 2021/03/22 21:39
 */
public class ThinClientKV {
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("localhost:10800");

        ClientCacheConfiguration clientCacheCfg = new ClientCacheConfiguration();
        clientCacheCfg.setName("Reference")
                .setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        try(IgniteClient client = Ignition.startClient(cfg)) {
            // 获取指定名字的缓存，如果不存在则会创建该缓存，创建时会使用提供的配置。
            ClientCache<Integer, String> cache = client.getOrCreateCache(clientCacheCfg);

            cache.put(1, "lzj_1");
            cache.put(2, "lzj_2");
            cache.put(3, "3_lzj");

            // query是以页的方式返回。可以在query中设置页大小: query.setPageSize(1024);
            ScanQuery<Integer, String> query = new ScanQuery<>((i, p) -> {
                return p.startsWith("lzj");
            });

            QueryCursor<Cache.Entry<Integer, String>> result = cache.query(query);
            result.forEach(entry -> {
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
