package com.epoint.ztb.bigdata.tagmg.constants;

public enum KeyType
{

    基础("basic"), SQL("sql");

    private String value;

    KeyType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static KeyType getEnumByValue(String value) {
        KeyType[] types = values();
        for (KeyType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return KeyType.基础;
    }
}
