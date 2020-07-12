package com.lzj;

import org.pf4j.Extension;

/**
 * Hello world!
 */
@Extension
public class FirstGreetingImpl implements Greeting {
    @Override
    public String greeting() {
        return "hello pf4j first";
    }
}
