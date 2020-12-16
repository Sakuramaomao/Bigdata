package com.example.service;

import com.lzj.api.TestInterface2;
import org.apache.dubbo.config.annotation.Service;
import org.springframework.stereotype.Component;

/**
 * @Author Sakura
 * @Date 2020/12/06 20:23
 */
@Service
@Component
public class TestImpl2 implements TestInterface2 {
    @Override
    public String test() {
        return "server-test-impl2";
    }
}
