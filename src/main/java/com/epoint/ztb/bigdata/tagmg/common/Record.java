package com.epoint.ztb.bigdata.tagmg.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

public class Record extends LinkedHashMap<String, Object>
{
    private static final long serialVersionUID = -6385409846924677939L;

    @Override
    public Object put(String key, Object value) {

        return super.put(key.toLowerCase(), value);
    }

    public void putAll(Record m) {

        super.putAll(m);
    }

    @Override
    public Object get(Object key) {

        return super.get(key.toString().toLowerCase());
    }

    public String getStr(Object key) {
        Object val = super.get(key.toString().toLowerCase());
        return val == null ? null : val.toString();
    }

    public Integer getInt(Object key) {
        Object val = super.get(key.toString().toLowerCase());
        Integer i = 0;
        if (val != null) {
            if (val instanceof Integer) {
                i = (Integer) val;
            }
            else {
                try {
                    i = Integer.parseInt(val.toString());
                }
                catch (Exception e) {

                }
            }
        }
        return i;
    }

    public Long getLong(Object key) {
        Object val = super.get(key.toString().toLowerCase());
        Long i = 0L;
        if (val != null) {
            if (val instanceof Long) {
                i = (Long) val;
            }
            else {
                try {
                    i = Long.parseLong(val.toString());
                }
                catch (Exception e) {

                }
            }
        }
        return i;
    }

    public Double getDouble(Object key) {
        Object val = super.get(key.toString().toLowerCase());
        Double d = 0.0;
        if (val != null) {
            if (val instanceof Double || val instanceof Integer) {
                d = (Double) val;
            }
            else {
                try {
                    d = Double.parseDouble(val.toString());
                }
                catch (Exception e) {

                }
            }
        }
        return d;
    }

    public Date getDate(Object key) {
        Object val = super.get(key.toString().toLowerCase());
        Date date = null;
        if (val != null) {
            if (val instanceof Date) {
                date = (Date) val;
            }
            else {
                String str = val.toString();
                if (str.matches("^[0-9]+$")) {
                    date = new Date(Integer.parseInt(str));
                }
                else {
                    String[] formats = new String[] {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd" };
                    for (String format : formats) {
                        SimpleDateFormat sdf = new SimpleDateFormat(format);
                        try {
                            date = sdf.parse(str);
                        }
                        catch (ParseException e) {

                        }
                        if (date != null) {
                            break;
                        }
                    }
                }
            }
        }
        return date;
    }
}
