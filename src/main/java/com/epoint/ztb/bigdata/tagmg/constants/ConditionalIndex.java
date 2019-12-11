package com.epoint.ztb.bigdata.tagmg.constants;

public enum ConditionalIndex
{
    条件索引(1), 非条件索引(2);

    private int value;

    ConditionalIndex(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
