package com.example.service;

import com.lzj.api.TestInterface;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.stereotype.Component;

/**
 * @Author Sakura
 * @Date 2020/12/06 20:23
 */
@DubboService
@Component
public class TestImpl implements TestInterface {
    @Override
    public String test() {
        return "server-test-impl";
    }
}
