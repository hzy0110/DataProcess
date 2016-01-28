package com.hzy.entity;

import org.apache.hadoop.io.WritableComparable;

import javax.lang.model.element.Name;

/**
 * Created by zy on 2016/1/28.
 */
public class ChineseMedicine{
    private String name;
    private Float maxPrice;
    private Float minPrice;
    private Float avgPrice;
    private Integer total;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getMaxPrice() {
        return this.maxPrice;
    }

    public void setMaxPrice(Float maxPrice) {
        this.maxPrice = maxPrice;
    }

    public Float getMinPrice() {
        return this.minPrice;
    }

    public void setMinPrice(Float minPrice) {
        this.minPrice = minPrice;
    }

    public Float getAvgPrice() {
        return this.avgPrice;
    }

    public void setAvgPrice(Float avgPrice) {
        this.avgPrice = avgPrice;
    }

    public Integer getTotal(){
        return this.total;
    }

    public void setTotal(Integer total){
        this.total=total;
    }

}
