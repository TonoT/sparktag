package com.epoint.ztb.bigdata.tagmg.constants;

public enum PostprocessingType
{
    不处理("none"), 权重型("weight"), 数值型("number");

    private String value;

    PostprocessingType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static PostprocessingType getEnumByValue(String value) {
        PostprocessingType[] types = values();
        for (PostprocessingType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return PostprocessingType.不处理;
    }
}
