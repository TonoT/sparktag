package com.epoint.ztb.bigdata.tagmg.constants;

public enum AccessType
{
    变量("variable"), 常量("constant"), 指标项("tagitem");

    private String value;

    AccessType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static AccessType getEnumByValue(String value) {
        AccessType[] types = values();
        for (AccessType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return AccessType.常量;
    }
}
