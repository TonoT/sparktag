package com.epoint.ztb.bigdata.tagmg.constants;

public enum JobType
{
    不需要定时(""), 时间间隔("timeinterval"), 每天("everyday"), 每周("everyweek"), 每月("everymonth"), 自定义Cron("cron");

    private String value;

    JobType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static JobType getEnumByValue(String value) {
        value = value == null ? "" : value;
        JobType[] types = values();
        for (JobType type : types) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return JobType.不需要定时;
    }
}
