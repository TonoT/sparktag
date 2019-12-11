package com.epoint.ztb.bigdata.tagmg.constants;

public enum ExtractType
{
    全量(1), 增量(2), 清除后全量(3);

    private int value;

    ExtractType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

}
