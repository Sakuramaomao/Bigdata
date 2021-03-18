package com.example.monitor;

/**
 * 测试staticObj、instanceObj和localObj都存放在哪里。
 *
 * @Author Sakura
 * @Date 2021/03/07 11:07
 */
public class JhsdbTest {
    public static void main(String[] args) {
        LzjTest lzjTest = new LzjTest();
        lzjTest.foo();
    }

    static class LzjTest {
       static LzjObjectHolder staticObj = new LzjObjectHolder();
        LzjObjectHolder instanceObj = new LzjObjectHolder();
        void foo () {
            LzjObjectHolder staticObj = new LzjObjectHolder();
            System.out.println("done");
        }
    }

    private static class LzjObjectHolder {
        private String name = "lzj";
        private static final int age = 18;
    }
}

//scanoops 0x0000000012200000 0x00000000124b0000 com.example.monitor.JhsdbTest$LzjObjectHolder