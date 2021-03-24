package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import java.util.List;

/**
 * <pre>
 *     使用瘦客户端执行SQL语句。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/23 22:13
 */
public class d_ThinClientSQL {
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("127.0.0.1:10800");

        IgniteClient client = Ignition.startClient(cfg);

        // 创建表
        client.query(new SqlFieldsQuery(String.format(
                "CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE=%s\"",
                Person.class.getName())).setSchema("PUBLIC")).getAll();

        // insert
        int key = 1;
        Person person = new Person(key, "person-1");
        client.query(new SqlFieldsQuery("INSERT INTO Person(id, name) VALUES (?, ?)").setArgs(person.id, person.name)
                .setSchema("PUBLIC")).getAll();

        // 查询
        // cursor 使用完毕后一定要注意关闭！！！
        FieldsQueryCursor<List<?>> cursor = client.query(new SqlFieldsQuery("Select name from Person where id = ?")
                .setArgs(key).setSchema("PUBLIC"));

        // getAll()是获取全部数据，所以不需要手动关闭cursor了！
        List<List<?>> results = cursor.getAll();

        results.stream().findFirst().ifPresent(columns -> {
            System.out.println("name = " + columns.get(0));
        });

    }
}
