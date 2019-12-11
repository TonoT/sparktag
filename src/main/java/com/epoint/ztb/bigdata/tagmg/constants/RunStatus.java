package com.epoint.ztb.bigdata.tagmg.constants;

public enum RunStatus
{
    执行中("running"), 执行成功("success"), 执行失败("failed"), 超时("timeout"), 被停止("closed");

    private String value;

    RunStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static RunStatus getEnumByValue(String value) {
        RunStatus[] types = values();
        for (RunStatus type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return RunStatus.执行中;
    }
}
