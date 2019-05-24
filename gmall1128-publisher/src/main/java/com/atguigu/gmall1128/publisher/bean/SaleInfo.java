package com.atguigu.gmall1128.publisher.bean;

import java.util.HashMap;
import java.util.List;

/**
 * 整个统计数据的结果
 */
public class SaleInfo {
    Integer total;  //总数

    List<OptionGroup> stat; //饼图集合

    List<HashMap> detail; //明细

    HashMap aggsMap;  //临时保存聚合结果

    public SaleInfo(Integer total, List<OptionGroup> stat, List<HashMap> detail,HashMap aggsMap) {
        this.total = total;
        this.stat = stat;
        this.detail = detail;
        this.aggsMap=aggsMap;
    }

    public HashMap getAggsMap() {
        return aggsMap;
    }

    public void setAggsMap(HashMap aggsMap) {
        this.aggsMap = aggsMap;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<OptionGroup> getStat() {
        return stat;
    }

    public void setStat(List<OptionGroup> stat) {
        this.stat = stat;
    }

    public List<HashMap> getDetail() {
        return detail;
    }

    public void setDetail(List<HashMap> detail) {
        this.detail = detail;
    }
}
