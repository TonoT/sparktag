package com.epoint.ztb.bigdata.tagmg.iface;

import java.util.Map;

import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;

public interface PreparedFormula
{

    /**
     * 
     * @param params
     *            指标项参数
     * @return
     */
    public Object run(Map<String, Object> params, ColumnType returntype);
}
