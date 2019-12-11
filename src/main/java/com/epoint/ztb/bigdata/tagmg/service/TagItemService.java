package com.epoint.ztb.bigdata.tagmg.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.SqlBuilder;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.computation.common.ComputationalUtils;
import com.epoint.ztb.bigdata.tagmg.factory.DBMetadataFactory;

public class TagItemService
{
    private TagCommonDao service;

    public TagItemService(TagCommonDao service) {
        this.service = service;
    }

    public Map<Record, List<Record>> getTagItems(String tagguid, boolean isNeedParams) {
        return getTagItems(tagguid, isNeedParams, "*", null, true);
    }

    /**
     * 获取顺序的指标项
     * LinkedHashMap实现
     * 
     * @param tagguid
     * @return Map<TagmgTagItem, List<TagmgTagItemParam>>
     */
    public Map<Record, List<Record>> getTagItems(String tagguid, boolean isNeedParams, String columns,
            List<String> itemanothernames, boolean onlyEnabled) {
        TagItemCategoryService categoryservice = new TagItemCategoryService(service);
        List<String> categoryguids = categoryservice.getCategoryguidList(tagguid);

        Map<Record, List<Record>> tagitems = new LinkedHashMap<Record, List<Record>>();

        for (String categoryguid : categoryguids) {
            SqlBuilder sqlbuilder = new SqlBuilder();
            sqlbuilder.append("select " + columns + " from tagmg_tag_item where itemcategoryguid = ?", categoryguid);
            if (onlyEnabled) {
                sqlbuilder.append(" and isenabled = 1");
            }
            if (itemanothernames != null && !itemanothernames.isEmpty()) {
                sqlbuilder.appendIn(" and itemanothername in (?)", itemanothernames);
            }
            sqlbuilder.append(" order by orderno desc");
            List<Record> items = service.findList(sqlbuilder.getSql(), sqlbuilder.getParams());

            for (Record item : items) {
                tagitems.put(item,
                        isNeedParams ? getItemParams(item.getStr("itemtype"), item.getStr("itemguid")) : null);
            }
        }
        return tagitems;
    }

    /**
     * 获取指标项参数
     * 
     * @param itemtype
     * @param itemguid
     * @return List<TagmgTagItemParam>
     */
    private List<Record> getItemParams(String itemtype, String itemguid) {
        List<Record> params = null;
        if ("sql".equals(itemtype) || "method".equals(itemtype) || "formula".equals(itemtype)) {
            params = service.findList("select * from tagmg_tag_item_param where itemguid = ?", itemguid);
        }
        return params;
    }

    public Map<String, String> getCodeMap(String codename, String datasourceguid) {
        Record datasource = service.find("select * from tagmg_datasource where datasourceguid = ?", datasourceguid);
        return ComputationalUtils.getCodeMap(codename, DBMetadataFactory.getDataSourceConfig(datasource));
    }
}
