package com.lzj.example.h_sql;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;

/**
 * Ignite的SQL引擎使用H2数据库来解析和优化查询并生成执行计划。
 *
 * * 查询方式
 *  （1）如果缓存是分区模式，就是分布式查询。
 *  （2）如果缓存是复制模式，就是本地查询。
 *
 * * 模式
 *  Ignite具有一些默认模式，并且支持创建自定义模式。
 *  （1）SYS模式：包含很多和集群各种信息相关的系统视图，不能在此模式中创建表！
 *  （2）PUBLIC模式：未指定模式时的默认模式。
 *  （3）自定义模式化：
 *      a、可以在集群配置中指定模式。
 *      b、Ignite为每个缓存都创建了一个模式。
 *
 *  * 缓存和模式的关系
 *    通过SQL API创建的表，除了使用SQL语句访问，也可以使用KV API访问。
 *    对应的缓存名称，可以在CREATE TABLE语句的WITH子句的CACHE_NAME参数进行指定。
 *    如未指定，默认的缓存名称为SQL_<SCHEMA_NAME>_<TABLE_NAME>
 *
 *  * 索引
 *    Ignite会自动为每个缓存的主键和关联键字段创建索引。
 *
 * @Author Sakura
 * @Date 2021/03/24 22:22
 */
public class a_Introduction {
    public static void main(String[] args) {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        // 自定义模式。感觉模式很像是数据库的概念。
        final SqlConfiguration sqlCfg = new SqlConfiguration();
        sqlCfg.setSqlSchemas("MY_SCHEMA");
        cfg.setSqlConfiguration(sqlCfg);

        // 如果要通过JDBC接入Ignite数据库中指定的模式，那么可以在连接URL中指定模式名称。
        // jdbc:ignite:thin://127.0.0.1/MY_SCHEMA
    }
}
