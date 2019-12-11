package com.epoint.ztb.bigdata.tagmg.constants;

public enum ColumnType
{
    整型("int"), 浮点型("double"), 字符型("string"), 时间型("date"), 字节型("byte");

    private String value;

    ColumnType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static ColumnType getEnumByValue(String value) {
        ColumnType[] types = values();
        for (ColumnType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return ColumnType.字符型;
    }
}
