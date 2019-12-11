package com.epoint.ztb.bigdata.tagmg.computation.runner;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.RunnerLogger;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.computation.common.ComputationalOperate;
import com.epoint.ztb.bigdata.tagmg.constants.RunStatus;

public class LocalRunner
{
    private static final Logger logger = Logger.getLogger(LocalRunner.class);

    public static String run(String modelguid) {
        String logguid = RunnerLogger.info(modelguid);
        new Thread(new Runnable()
        {
            @Override
            public void run() {
                start(modelguid, logguid, "local[*]");
            }
        }).start();

        return logguid;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.info("参数异常！第一个参数必须：modelguid。第二个参数可选：master");
        }
        else {
            String modelguid = args[0];
            String master = null;
            if (args.length > 1) {
                master = args[1];
            }
            String logguid = RunnerLogger.info(modelguid);
            start(modelguid, logguid, master);
        }
    }

    private static void start(String modelguid, String logguid, String master) {
        Record runconfig = null;
        try (TagCommonDao service = TagCommonDao.getInstance(DSConfig.getDataSourceConfig());) {
            runconfig = service.find(
                    "select * from tagmg_runconfig where configguid in (select configguid from tagmg_model where modelguid = ?)",
                    modelguid);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Builder builder = SparkSession.builder().appName(modelguid);
        if (StringUtils.isNotBlank(master)) {
            builder = builder.master(master);
        }

        String configStr = runconfig.getStr("configcontent");
        if (StringUtils.isNotBlank(configStr)) {
            Map<String, String> conf = JSON.parseObject(configStr, new TypeReference<Map<String, String>>()
            {
            });
            for (Entry<String, String> entry : conf.entrySet()) {
                builder = builder.config(entry.getKey(), entry.getValue());
            }
        }
        SparkSession spark = builder.getOrCreate();

        if (runconfig.getInt("isdebug") != 1) {
            // 调试模式将日志全打出来，非调试模式下减少日志
            spark.sparkContext().setLogLevel("WARN"); // 防止太多日志
        }

        ComputationalOperate operate = new ComputationalOperate(spark);
        List<String> msgs = operate.operate(modelguid);
        if (msgs == null || msgs.isEmpty()) {
            RunnerLogger.setRunStatus(logguid, RunStatus.执行成功);
        }
        else {
            for (String msg : msgs) {
                // 打印报错日志
                // 这句必须有 解决异步的问题
                logger.info(msg);
            }
            RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, msgs);
        }
        // 关闭编程入口
        spark.close();
    }
}
