package com.epoint.ztb.bigdata.tagmg.computation.runner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.epoint.ztb.bigdata.tagmg.common.ClassPathUtil;
import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.Functions;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.RunnerLogger;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.computation.job.ComputationalJob;
import com.epoint.ztb.bigdata.tagmg.constants.RunStatus;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.google.common.io.Files;

public class SparkRunner
{
    private static final Logger logger = Logger.getLogger(SparkRunner.class);

    public static String run(String modelguid) {
        String logguid = RunnerLogger.info(modelguid);
        new Thread(new Runnable()
        {

            @Override
            public void run() {
                // 获取运行配置
                Record runconfig = getRunConfig(modelguid);

                if (runconfig == null) {
                    RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "未找到运行配置内容！");
                    return;
                }

                if (!packageJar()) {
                    RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "打包失败！");
                    return;
                }

                File jarsdir = new File(ClassPathUtil.getDeployWarPath() + "WEB-INF" + File.separator + "jars");

                List<File> cfiles = Functions.listFiles(jarsdir, null);

                File jar = null;
                for (File cfile : cfiles) {
                    if (SysConstant.JAR_NAME_WithSUFFIX.equals(cfile.getName())) {
                        jar = cfile;
                        break;
                    }
                }

                // 找到计算包
                if (!jar.exists()) {
                    RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "未找到计算包！" + jar.getPath());
                    return;
                }

                // 导入jdbc配置文件
                if (!copyJDBCToJar(jar)) {
                    RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "jdbc配置文件导入失败！");
                    return;
                }

                // 获取运行配置
                Map<String, String> conf = null;
                String configStr = runconfig.getStr("configcontent");
                if (StringUtils.isNotBlank(configStr)) {
                    conf = JSON.parseObject(configStr, new TypeReference<Map<String, String>>()
                    {
                    });
                }
                else {
                    conf = new HashMap<String, String>();
                }

                // 上传
                String livyurl = "http://" + runconfig.get("ip") + ":" + runconfig.get("port");
                LivyClient client = null;
                String sessionId = null;
                try {
                    LivyClientBuilder builder = new LivyClientBuilder().setURI(new URI(livyurl));
                    for (Entry<String, String> entry : conf.entrySet()) {
                        builder = builder.setConf(entry.getKey(), entry.getValue());
                    }
                    client = builder.build();

                    Method method = client.getClass().getDeclaredMethod("getSessionId");
                    method.setAccessible(true);
                    Object obj = method.invoke(client);

                    if (obj != null) {

                        File hbase_site_xml = new File(Functions.getClassPath() + File.separator + "hbase-site.xml");
                        if (hbase_site_xml.exists()) {
                            client.uploadJar(hbase_site_xml).get();
                        }
                        else {
                            logger.info("未找到hbase-site.xml配置文件！文件路径：" + hbase_site_xml.getPath());
                        }

                        for (File cfile : cfiles) {
                            client.uploadJar(cfile).get();
                        }

                        sessionId = obj.toString();
                        RunnerLogger.setSessionId(logguid, sessionId);
                    }
                }
                catch (Exception e) {
                    RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "上传计算包时发生异常！异常信息：" + e.getMessage());
                    logger.error("上传计算包时发生异常！", e);
                }
                finally {
                    if (client != null) {
                        client.stop(false);
                    }
                }

                // 计算
                if (StringUtils.isNotBlank(sessionId)) {
                    livyurl = livyurl + "/sessions/" + sessionId;
                    try {
                        client = new LivyClientBuilder().setURI(new URI(livyurl)).build();
                        List<String> msgs = client.submit(new ComputationalJob(modelguid, logguid)).get();

                        for (String msg : msgs) {
                            // 打印报错日志
                            // 这句必须有 解决异步的问题
                            logger.info(msg);
                        }
                    }
                    catch (Exception e) {
                        RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "执行计算包时发生异常！异常信息：" + e.getMessage());
                        logger.error("执行计算包时发生异常！", e);
                    }
                    finally {
                        if (client != null) {
                            client.stop(false);
                        }
                    }
                }
                else {
                    RunnerLogger.setRunStatus(logguid, RunStatus.执行失败, "未成功获取到sessionId！");
                }

                // session关闭交给job执行！
            }
        }).start();

        return logguid;

    }

    /**
     * 获取运行配置
     * 
     * @param modelguid
     * @return
     */
    public static Record getRunConfig(String modelguid) {
        Record runconfig = null;
        try (TagCommonDao service = TagCommonDao.getInstance(DSConfig.getDataSourceConfig());) {

            runconfig = service.find(
                    "select * from tagmg_runconfig where configguid in (select configguid from tagmg_model where modelguid = ?)",
                    modelguid);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return runconfig;
    }

    /**
     * 开发环境下打包jar包
     * 
     * @return
     */
    public static boolean packageJar() {
        String cmdParentPath = Functions.getProjectPath() + File.separator + ".." + File.separator + ".."
                + File.separator + "SubSys" + File.separator + SysConstant.JAR_NAME;
        boolean isDev = new File(cmdParentPath + File.separator + "pom.xml").exists();
        boolean isSuccess = false;
        if (isDev) {
            // 开发环境
            isSuccess = Functions.cmd(cmdParentPath + File.separator + "package.bat");
            if (isSuccess) {
                File jar = new File(
                        cmdParentPath + File.separator + "target" + File.separator + SysConstant.JAR_NAME_WithSUFFIX);

                File to = new File(ClassPathUtil.getDeployWarPath() + "WEB-INF" + File.separator + "jars"
                        + File.separator + SysConstant.JAR_NAME_WithSUFFIX);

                try {
                    Files.copy(jar, to);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    isSuccess = false;
                }
            }
        }
        else {
            isSuccess = true;
        }
        return isSuccess;
    }

    /**
     * Jar中如果不存在jdbc.properties文件，复制进去
     */
    public static boolean copyJDBCToJar(File jar) {
        if (Functions.existsInJar(jar, "^jdbc.properties$")) {
            return true;
        }
        // 导入jdbc文件
        File jdbc = new File(Functions.getClassPath() + File.separator + "jdbc.properties");
        if (!jdbc.exists()) {
            return false;
        }
        // 将JDBC文件放入Jar包中。
        try {
            Functions.updateJarFile(jar, jdbc);
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
