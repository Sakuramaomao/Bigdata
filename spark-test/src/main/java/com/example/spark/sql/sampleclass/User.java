package com.example.spark.sql.sampleclass;

import java.io.Serializable;

/**
 * <pre>
 * 创建Dataset时需要使用的样例类。
 *
 * 样例类的几点要求：
 *    1、必须是public权限。
 *    2、必须有setter、getter方法。
 *    3、序列化。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/8/4 21:47
 */
public class User implements Serializable {
    private int id;
    private String name;
    private int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
