package com.atguigu.gmall1128.publisher.bean;


import java.util.List;

/***
 * 对应某个饼图
 */
public class OptionGroup {

    List<Option>  options;
    String  title ;

    public OptionGroup(List<Option> options, String title) {
        this.options = options;
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
