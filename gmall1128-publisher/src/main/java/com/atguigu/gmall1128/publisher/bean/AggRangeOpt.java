package com.atguigu.gmall1128.publisher.bean;

public class AggRangeOpt {
        Double from ;
        Double to;
        String key;

    public AggRangeOpt(Double from, Double to, String key) {
        this.from = from;
        this.to = to;
        this.key = key;
    }

    public Double getFrom() {
        return from;
    }

    public void setFrom(Double from) {
        this.from = from;
    }

    public Double getTo() {
        return to;
    }

    public void setTo(Double to) {
        this.to = to;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
