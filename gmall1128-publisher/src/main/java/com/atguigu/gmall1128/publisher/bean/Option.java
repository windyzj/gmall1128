package com.atguigu.gmall1128.publisher.bean;


/**
 * 对应饼图中的各个选项
 */
public class Option {

     String name ;
     Double value;

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
