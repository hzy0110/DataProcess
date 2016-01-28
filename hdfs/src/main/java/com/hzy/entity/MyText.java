package com.hzy.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zy on 2016/1/28.
 */
public class MyText implements WritableComparable {
    private String key = "";
    private int value = 0;
    public MyText() {
    }
    public MyText(String key, int value) {
        this.key = key;
        this.value = value;
    }
    @Override
    //反序列化，从流中的二进制转换成IntPair
    public void readFields(DataInput in) throws IOException {
        key = in.readUTF();
        value = in.readInt();
    }
    @Override
    //序列化，将IntPair转化成使用流传送的二进制
    public void write(DataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeInt(value);
    }
    @Override
    //key的比较
    public int compareTo(Object o) {
        MyText other = (MyText) o;
        return -this.key.compareTo(other.key);
    }
    public int CompareToValue(Object o) {
        MyText other = (MyText) o;
        return this.value - other.value;
    }
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }
    public int getValue() {
        return value;
    }
    public void setValue(int value) {
        this.value = value;
    }
}