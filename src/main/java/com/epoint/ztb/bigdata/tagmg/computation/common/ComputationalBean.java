package com.epoint.ztb.bigdata.tagmg.computation.common;

import java.util.List;
import java.util.Map;

import com.epoint.ztb.bigdata.tagmg.common.Record;

public class ComputationalBean
{
    private Record tag;
    private Record dataSourceConfig;
    private Map<Record, List<Record>> stagingTables;

    private Map<Record, List<Record>> tagItems;

    public ComputationalBean() {
        super();
    }

    public ComputationalBean(Record tag, Record dataSourceConfig, Map<Record, List<Record>> stagingTables,
            Map<Record, List<Record>> tagItems) {
        super();
        this.tag = tag;
        this.dataSourceConfig = dataSourceConfig;
        this.stagingTables = stagingTables;
        this.tagItems = tagItems;
    }

    public Record getTag() {
        return tag;
    }

    public void setTag(Record tag) {
        this.tag = tag;
    }

    public Record getDataSourceConfig() {
        return dataSourceConfig;
    }

    public void setDataSourceConfig(Record dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
    }

    public Map<Record, List<Record>> getStagingTables() {
        return stagingTables;
    }

    public void setStagingTables(Map<Record, List<Record>> stagingTables) {
        this.stagingTables = stagingTables;
    }

    public Map<Record, List<Record>> getTagItems() {
        return tagItems;
    }

    public void setTagItems(Map<Record, List<Record>> tagItems) {
        this.tagItems = tagItems;
    }

}
