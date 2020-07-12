package com.lzj;

import org.pf4j.Extension;

/**
 * @Author Sakura
 * @Date 2020/4/15 18:04
 */
@Extension
public class SecondGreetingImpl implements Greeting{
    @Override
    public String greeting() {
        return "hello pf4j second";
    }
}
