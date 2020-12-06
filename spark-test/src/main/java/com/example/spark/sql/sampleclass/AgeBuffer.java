package com.example.spark.sql.sampleclass;

import java.io.Serializable;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/8/5 11:32
 **/
public class AgeBuffer implements Serializable {
    private long totalAge;
    private long count;

    public AgeBuffer() {
    }

    public AgeBuffer(long totalAge, long count) {
        this.totalAge = totalAge;
        this.count = count;
    }

    public long getTotalAge() {
        return totalAge;
    }

    public void setTotalAge(long totalAge) {
        this.totalAge = totalAge;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
