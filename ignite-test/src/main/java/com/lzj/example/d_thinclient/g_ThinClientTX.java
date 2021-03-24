package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.*;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientTransactionConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * <pre>
 *     练习瘦客户端使用事务。
 *
 *     可以使用${@link ClientTransactionConfiguration}来配置cache的事务并发模型。
 *     可以在一个事务里单独配置，此时的配置会覆盖默认配置。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/23 21:18
 */
public class g_ThinClientTX {
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("localhost:10800");

        // 配置事务的并发模型，事务超时时间和事务隔离级别。
        ClientTransactionConfiguration txCfg = new ClientTransactionConfiguration();
        txCfg.setDefaultTxTimeout(1000)
                .setDefaultTxConcurrency(TransactionConcurrency.OPTIMISTIC)
                .setDefaultTxIsolation(TransactionIsolation.REPEATABLE_READ);

        cfg.setTransactionConfiguration(txCfg);
        // start Client with tx。
        IgniteClient client = Ignition.startClient(cfg);

        ClientCache<Integer, String> txCache = client.getOrCreateCache(new ClientCacheConfiguration()
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).setName("tx_cache"));

        // 开启事务。
        ClientTransactions tx = client.transactions();
        // txStart()中可以单独对这个事务配置并发模型。
        try (ClientTransaction t = tx.txStart()) {
            txCache.put(1, "lzj_1");

            // 提交。
            t.commit();
            System.out.println("事务提交成功");
        }



    }
}
