package com.epoint.ztb.bigdata.tagmg.computation.runner;

import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.constants.RunType;

public class RunnerFactory
{
    public static String run(String modelguid) {
        String runtype = null;
        try (TagCommonDao service = TagCommonDao.getInstance(DSConfig.getDataSourceConfig());) {
            runtype = service.queryString(
                    "select runtype from tagmg_runconfig where configguid in (select configguid from tagmg_model where modelguid = ?)",
                    modelguid);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return RunType.分布式模式.getValue().equals(runtype) ? SparkRunner.run(modelguid) : LocalRunner.run(modelguid);
    }
}
