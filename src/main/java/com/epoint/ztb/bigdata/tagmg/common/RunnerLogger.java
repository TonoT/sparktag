package com.epoint.ztb.bigdata.tagmg.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.epoint.ztb.bigdata.tagmg.constants.RunStatus;
import com.epoint.ztb.bigdata.tagmg.constants.SessionStatus;

public class RunnerLogger
{
    private final static Logger logger = Logger.getLogger(RunnerLogger.class);

    public static String info(String modelguid) {
        String rowguid = UUID.randomUUID().toString();
        logger.info("运行模型：" + modelguid);

        try (TagCommonDao service = TagCommonDao.getInstance()) {
            Record model = service.find("select * from tagmg_model where modelguid = ?", modelguid);
            Record runconfig = service.find("select * from tagmg_runconfig where configguid = ?",
                    model.getStr("configguid"));

            String sql = "insert into tagmg_runlog(rowguid, logguid, modelguid, modelname, modelremarks,"
                    + " configguid, configname, configremarks, runstatus, sessionstatus, operatedate)"
                    + " values(?,?,?,?,?,?,?,?,?,?,?)";

            service.execute(sql, rowguid, rowguid, model.getStr("modelguid"), model.getStr("modelname"),
                    model.getStr("remarks"), runconfig.getStr("configguid"), runconfig.getStr("configname"),
                    runconfig.getStr("remarks"), RunStatus.执行中.getValue(), SessionStatus.关闭.getValue(), new Date());

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return rowguid;
    }

    public static void setSessionId(String logguid, String sessionId) {
        logger.info("日志标识：" + logguid + "，sessionId：" + sessionId);

        try (TagCommonDao service = TagCommonDao.getInstance()) {
            service.execute("update tagmg_runlog set sessionid = ?, sessionstatus = ? where logguid = ?", sessionId,
                    SessionStatus.开启.getValue(), logguid);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setRunStatus(String logguid, RunStatus status, List<String> msgs) {
        logger.info("日志标识：" + logguid + "，状态：" + status);

        try (TagCommonDao service = TagCommonDao.getInstance()) {
            String runmsgsStr = service.queryString("select runmsgs from tagmg_runlog where logguid = ?", logguid);
            List<String> msgsList = null;
            if (msgs != null && !msgs.isEmpty()) {
                if (StringUtils.isNotBlank(runmsgsStr)) {
                    msgsList = JSON.parseArray(runmsgsStr, String.class);
                    msgsList.addAll(msgs);
                }
                else {
                    msgsList = msgs;
                }
            }
            service.execute(
                    "update tagmg_runlog set runstatus = ?, completiondate = ?, runmsgs = ?   where logguid = ?",
                    status.getValue(), new Date(), msgsList == null ? null : JSON.toJSONString(msgsList), logguid);
        }
    }

    public static void setRunStatus(String logguid, RunStatus status, String... msgs) {
        List<String> msgList = new ArrayList<String>(msgs.length);
        Collections.addAll(msgList, msgs);
        setRunStatus(logguid, status, msgList);
    }
}
