package com.epoint.ztb.bigdata.tagmg.computation.job;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.sql.SparkSession;

import com.epoint.ztb.bigdata.tagmg.common.RunnerLogger;
import com.epoint.ztb.bigdata.tagmg.computation.common.ComputationalOperate;
import com.epoint.ztb.bigdata.tagmg.constants.RunStatus;

public class ComputationalJob implements Job<List<String>>
{

    private static final long serialVersionUID = -3847305631826667462L;

    private String modelguid;

    private String logguid;

    public ComputationalJob(String modelguid, String logguid) {
        this.modelguid = modelguid;
        this.logguid = logguid;
    }

    @Override
    public List<String> call(JobContext ctx) throws Exception {
        SparkSession spark = ctx.sparkSession();
        List<String> msgs = new ComputationalOperate(spark).operate(modelguid);

        // 是否成功的标记位放到job中执行 这样即使指标库停止运行 也能置位。
        if (StringUtils.isNotBlank(logguid)) {
            if (msgs == null || msgs.isEmpty()) {
                RunnerLogger.setRunStatus(logguid, RunStatus.执行成功);
            }
            else {
                RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, msgs);
            }
        }

        return msgs;
    }
}
