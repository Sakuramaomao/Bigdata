package com.lzj.example.f_kvapi;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * <pre>
 *     基本缓存操作。
 *
 *     * 异步执行获取缓存结果
 *       异步操作会返回一个代表操作结果的对象，可以以阻塞或者非阻塞的方式，等待操作的完成。
 *       以非阻塞的方式等待结果，可以使用IgniteFuture.listen()或者IgniteFuture.chain()方法注册一个闭包，
 *       其会在操作完成后被调用。
 *
 *       注意：因此应避免在闭包内部调用同步缓存和计算操作，因为由于线程池不足，它可能导致死锁。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/24 21:55
 */
public class a_BasicCacheOperation {
    public static void main(String[] args) {
        Ignite ignite = Ignition.start();

        // 直接获取或者创建缓存。
        IgniteCache<Object, Object> myCache = ignite.getOrCreateCache("myCache");

        // 创建具有事务的缓存。
        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("txCache");
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ignite.getOrCreateCache(cacheCfg);

        // 缓存的异步执行操作。
        IgniteFuture<Object> person = myCache.getAsync("person");
        person.listen(new IgniteInClosure<IgniteFuture<Object>>() {
            // 非阻塞的方式获取结果。
            @Override
            public void apply(IgniteFuture<Object> future) {
                future.get();
            }
        });

        // 销毁缓存。
        myCache.destroy();

    }
}
