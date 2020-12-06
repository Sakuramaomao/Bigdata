package com.example.service;

import com.alibaba.dubbo.config.annotation.Service;
import com.lzj.api.TestInterface;
import org.springframework.stereotype.Component;

/**
 * @Author Sakura
 * @Date 2020/12/06 20:23
 */
@Service
@Component
public class TestImpl implements TestInterface {
    @Override
    public String test() {
        return "server-test-impl";
    }
}
