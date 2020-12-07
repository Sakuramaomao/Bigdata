package com.example.service;

import com.lzj.api.Test3Interface;
import org.apache.dubbo.config.annotation.Service;
import org.springframework.stereotype.Component;

/**
 * @Author Sakura
 * @Date 2020/12/06 20:23
 */
@Service
@Component
public class Test3Impl implements Test3Interface {
    @Override
    public String test() {
        return "server-test2-impl";
    }
}
