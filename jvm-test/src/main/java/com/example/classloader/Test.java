package com.example.classloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author Sakura
 * @Date 2020/1/5 21:58
 */
public class Test {
    //private static final String name = "lzj";
    //private static final int age = 27;

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/day72?serverTimezone=UTC", "root", "123456");

        //ClassLoader cl = Thread.currentThread().getContextClassLoader();
        //System.out.println(cl);
    }

}

