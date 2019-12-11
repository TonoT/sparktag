package com.epoint.ztb.bigdata.tagmg.iface;

import java.util.List;
import java.util.Map;

import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;

public interface TagItemContent_ReturnMap
{
    /**
     * 
     * @param tagitem
     * @param spark
     * @param data
     *            增量数据
     * @return 返回为 key-value的Map
     */
    public Map<Object, Object> call(Record tagitem, TagCommonDao service, List<Record> datalist,
            Map<String, Object> params);
}
