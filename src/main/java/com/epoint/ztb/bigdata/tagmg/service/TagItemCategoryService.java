package com.epoint.ztb.bigdata.tagmg.service;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;

public class TagItemCategoryService
{
    private TagCommonDao service;

    public TagItemCategoryService(TagCommonDao service) {
        this.service = service;
    }

    /**
     * 获取顺序的指标项分类
     * 
     * @param tagguid
     * @return
     */
    public List<String> getCategoryguidList(String tagguid) {
        List<String> categoryguids = new LinkedList<String>();
        List<Record> itemcategories = service.findList(
                "select itemcategoryguid from tagmg_tag_item_category where tagguid = ? and (parentguid is null or parentguid = '') order by orderno desc",
                tagguid);
        for (Record itemcategory : itemcategories) {
            String itemcategoryguid = itemcategory.getStr("itemcategoryguid");
            categoryguids.add(itemcategoryguid);
            getCategoryList(itemcategoryguid, categoryguids);
        }
        return categoryguids;
    }

    /**
     * 获取顺序的指标项分类
     * 
     * @param itemcategoryguid
     * @param categoryguids
     */
    public void getCategoryList(String itemcategoryguid, List<String> categoryguids) {
        List<Record> itemcategories = service.findList(
                "select itemcategoryguid from tagmg_tag_item_category where parentguid = ? order by orderno desc",
                itemcategoryguid);
        if (!itemcategories.isEmpty()) {
            for (Record itemcategory : itemcategories) {
                String guid = itemcategory.getStr("itemcategoryguid");
                categoryguids.add(guid);
                getCategoryList(guid, categoryguids);
            }
        }
    }

    /**
     * 获取某个类别的所有父类
     * 
     * @param categoryguid
     * @return List<TagmgTagItemCategory>
     */
    public List<Record> getAllParentCategories(String categoryguid) {
        List<Record> lst = new LinkedList<Record>();
        if (StringUtils.isNotBlank(categoryguid)) {
            Record category = service.find("select * from tagmg_tag_item_category where itemcategoryguid = ?",
                    categoryguid);
            if (category != null) {
                lst.add(0, category);
            }
            if (StringUtils.isNotBlank(category.getStr("parentguid"))) {
                getAllParentCategories(lst, category.getStr("parentguid"));
            }
        }
        return lst;
    }

    /**
     * 获取某个类别的所有父类
     * 
     * @param lst
     * @param categoryguid
     */
    private void getAllParentCategories(List<Record> lst, String categoryguid) {
        if (StringUtils.isNotBlank(categoryguid)) {
            Record category = service.find("select * from tagmg_tag_item_category where itemcategoryguid = ?",
                    categoryguid);
            if (category != null) {
                lst.add(0, category);
            }
            if (StringUtils.isNotBlank(category.getStr("parentguid"))) {
                getAllParentCategories(lst, category.getStr("parentguid"));
            }
        }
    }

}
