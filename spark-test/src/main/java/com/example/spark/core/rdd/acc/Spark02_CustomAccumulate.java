package com.example.spark.core.rdd.acc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *  自定义K-V类型的累加器。Spark自带的没有这一类型的累加器。
 *    利用累加器的方式来实现WordCount。
 *      1、创建累加器
 *      2、注册累加器
 *      3、使用累加器
 *      4、获取累加器的值
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/28 17:12
 **/
public class Spark02_CustomAccumulate {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("partitionBy-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkContext);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello scala", "hello java", "spark", "scala"));

        // 1、创建累加器
        MyAccumulate<String, Map<String, Integer>> myAccumulate = new MyAccumulate<>();

        // 2、注册累加器
        sparkContext.register(myAccumulate);

        // 3、使用累加器
        rdd.flatMap(line -> {
            String[] wordArr = line.split(" ");
            return Arrays.asList(wordArr).iterator();
        }).foreach(word -> {
            myAccumulate.add(word);
        });

        // 4、获取累加器的值
        System.out.println(myAccumulate.value());
    }
}

/**
 * 自定义K-V类型的累加器，实现WordCount例子。
 *
 * @param <S> In 输入累加器的值。
 * @param <M> Out 累加器的累加结果。
 */
class MyAccumulate<S, M> extends AccumulatorV2<S, M> {
    // 用来存放K-V值的累加器。
    HashMap<String, Integer> wordCountMap = new HashMap<>();

    // 判断累加器是否被使用过。
    @Override
    public boolean isZero() {
        return wordCountMap.isEmpty();
    }

    // 累加器复制
    @Override
    public AccumulatorV2<S, M> copy() {
        return new MyAccumulate<>();
    }

    // 重置累加器
    @Override
    public void reset() {
        wordCountMap.clear();
    }


    // 添加值到累加器中。
    @Override
    public void add(S s) {
        if (wordCountMap.containsKey(s)) {
            Integer v = wordCountMap.get(s);
            wordCountMap.put((String) s, v + 1);
        } else {
            wordCountMap.put((String) s, 1);
        }
    }

    // spark中Driver调用此方法来合并从Executor返回的累加器结果。
    @Override
    public void merge(AccumulatorV2<S, M> other) {
        HashMap<String, Integer> map1 = wordCountMap;
        HashMap<String, Integer> map2 = (HashMap) other.value();

        map2.forEach((k, v) -> {
            if (map1.containsKey(k)) {
                Integer v1 = map1.get(k);
                map1.put(k, v1 + v);
            } else {
                map1.put(k, v);
            }
        });

        // 注意需要将wordCountMap的引用修改成合并后的map。
        wordCountMap = map1;
    }

    // 获取累加器的值
    @Override
    public M value() {
        return (M) wordCountMap;
    }
}
