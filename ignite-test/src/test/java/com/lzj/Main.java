package com.lzj;

/**
 * @Author Sakura
 * @Date 2021/03/22 22:03
 */
class temp{
    private int i =30;
    temp(){
        System.out.println(this.get()+" "+this.i);
    }
    protected int get(){
        return 10;
    }
}
public class Main extends temp{
    private int i =40;
    public static void main(String[]args){
        Main i =new Main();
    }

    // 权限降低就报错
    @Override
    protected int get(){
        return 20;
    }

}