package com.epoint.ztb.bigdata.tagmg.constants;

public enum SessionStatus
{
    开启("open"), 关闭("close");

    private String value;

    SessionStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
