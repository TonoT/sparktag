package com.epoint.ztb.bigdata.tagmg.tagitemcontent.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.epoint.ztb.bigdata.tagmg.annotation.TagItemContent;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.iface.TagItemContent_ReturnMap;

@TagItemContent("TagItemContent_ReturnMap测试")
public class TagItemContent_ReturnMap_Test implements TagItemContent_ReturnMap
{

    @Override
    public Map<Object, Object> call(Record tagitem, TagCommonDao service, List<Record> datalist,
            Map<String, Object> params) {
        Map<Object, Object> map = new HashMap<Object, Object>();
        for (Record rec : datalist) {
            map.put(rec.getStr("danweiname"), "test");
        }
        return map;
    }

}
