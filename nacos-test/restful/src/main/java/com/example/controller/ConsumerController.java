package com.example.controller;

import com.lzj.api.TestInterface;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author Sakura
 * @Date 2020/12/06 18:03
 */
@RestController
public class ConsumerController {
    // 注入基于Dubbo协议的代理类对象。
    @Reference
    TestInterface testInterface;

    @GetMapping("/service")
    public String Service() {
        return testInterface.test();
    }
}
