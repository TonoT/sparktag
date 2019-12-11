package com.epoint.ztb.bigdata.tagmg.constants;

public enum ItemType
{

    主键("key"), 基础("basic"), SQL("sql"), 方法("method"), 公式("formula");

    private String value;

    ItemType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static ItemType getEnumByValue(String value) {
        ItemType[] types = values();
        for (ItemType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return ItemType.基础;
    }
}
