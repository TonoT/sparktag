package com.epoint.ztb.bigdata.tagmg.constants;

public enum CodeType
{
    数据源("datasource"), 指标库("local");

    public final static String SPLIT = ":";

    private String value;

    CodeType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
