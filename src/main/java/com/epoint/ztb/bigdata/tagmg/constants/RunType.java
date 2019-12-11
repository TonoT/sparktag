package com.epoint.ztb.bigdata.tagmg.constants;

public enum RunType
{
    本地模式("local"), 分布式模式("spark");

    private String value;

    RunType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static RunType getEnumByValue(String value) {
        RunType[] types = values();
        for (RunType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return RunType.本地模式;
    }
}
