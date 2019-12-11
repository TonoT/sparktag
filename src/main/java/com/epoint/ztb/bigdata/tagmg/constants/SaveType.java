package com.epoint.ztb.bigdata.tagmg.constants;

public enum SaveType
{
    MySQL("mysql"), HBase("hbase");

    private String value;

    SaveType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static SaveType getEnumByValue(String value) {
        SaveType[] types = values();
        for (SaveType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return SaveType.HBase;
    }
}
