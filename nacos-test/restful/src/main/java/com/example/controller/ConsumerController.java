package com.example.controller;

import com.lzj.api.TestInterface;
import com.lzj.api.TestInterface1;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author Sakura
 * @Date 2020/12/06 18:03
 */
@RestController
public class ConsumerController {
    // 注入基于Dubbo协议的代理类对象。
    @DubboReference
    private TestInterface testInterface;

    @DubboReference
    private TestInterface1 testInterface1;

    @GetMapping("/service")
    public String Service() {
        return testInterface.test();
    }

    @GetMapping("/servicetest")
    public String Service1() {
        return testInterface1.test();
    }
}
