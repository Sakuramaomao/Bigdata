package com.lzj.example.h_sql;

import com.lzj.example.d_thinclient.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.List;

/**
 * <pre>
 *   使用SQL API
 *
 *   * SqlFieldsQuery
 *      （1）这是个核心API，可以将INSERT、SELECT、DELETE、CREATE等语句放入这里，
 *      （2）默认在PUBLIC模式下执行，如果想指定模式，就需要使用setSchema("...")来设置。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2021/3/25 12:10
 **/
public class b_SqlAPI {
    public static void main(String[] args) {
        final Ignite ignite = Ignition.start();
        final IgniteCache<Integer, Person> myCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Person>().setName("MY_CACHE"));

        myCache.query(new SqlFieldsQuery("create table Person (id int primary key, name varchar)")).getAll();

        myCache.put(1, new Person(1, "lzj_1"));
        myCache.put(2, new Person(2, "lzj_2"));
        myCache.put(3, new Person(2, "lzj_3"));

        final SqlFieldsQuery query = new SqlFieldsQuery("select concat(id, '_', name) from MY_CACHE.Person");

        final FieldsQueryCursor<List<?>> cursor = myCache.query(query);
        cursor.forEach( row -> {
            System.out.println("person: " + row.get(0));
        });
    }
}
