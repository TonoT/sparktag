package com.epoint.ztb.bigdata.tagmg.formula.impl;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import com.epoint.ztb.bigdata.tagmg.annotation.Formula;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;
import com.epoint.ztb.bigdata.tagmg.iface.PreparedFormula;

@Formula("sum")
public class Sum implements PreparedFormula
{

    @Override
    public Object run(Map<String, Object> params, ColumnType returntype) {
        double sum = 0;
        for (Entry<String, Object> entry : params.entrySet()) {
            Object val = entry.getValue();
            if (val instanceof String) {
                try {
                    val = Double.parseDouble(val.toString());
                }
                catch (Exception e) {

                }
            }
            if (val instanceof Date) {
                val = ((Date) val).getTime();
            }
            if (val != null) {
                if (val instanceof Double) {
                    sum += (Double) val;
                }
                else if (val instanceof Float) {
                    sum += (Float) val;
                }
                else if (val instanceof Long) {
                    sum += (Long) val;
                }
                else if (val instanceof Integer) {
                    sum += (Integer) val;
                }
            }
        }

        switch (returntype) {
            case 整型:
                return Math.round(sum);
            case 浮点型:
                return sum;
            case 字符型:
                return sum + "";
            default:
                return null;

        }
    }

}
