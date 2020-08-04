package com.lzj;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void dateTest() throws ParseException {
        String str = "2020-07-22 16:30:20";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = format.parse(str);

    }

    @Test
    public void mapTest() {
        HashMap<String, Integer> map1 = new HashMap<>();
        HashMap<String, Integer> map2 = new HashMap<>();

        map1.put("hello", 1);
        map1.put("world", 1);
        map2.put("hello", 1);

        Integer hello = map1.merge("hello", 1, Integer::sum);
        System.out.println(hello);
    }

    @Test
    public void charTest() {
        char ch = '我';
        String str = "l";
    }
}
