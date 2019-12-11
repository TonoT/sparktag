package com.epoint.ztb.bigdata.tagmg.common;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class ClassPathUtil
{
    private static String warpath;
    public static final String MVN_TAG = "target/classes/";
    public static final String MVN_TEST_TAG = "target/test-classes/";
    private static transient Logger log = Logger.getLogger(ClassPathUtil.class);

    public static String getWebContext() {
        String projectName = null;
        try {
            String[] sb = getDeployWarPath().split("/");
            projectName = sb[(sb.length - 1)];
            if ("webcontent".equalsIgnoreCase(projectName)) {
                projectName = sb[(sb.length - 2)];
            }
            else if ("webapp".equalsIgnoreCase(projectName)) {
                projectName = sb[(sb.length - 4)];
            }
        }
        catch (Exception e) {
            projectName = "EpointJWeb";
        }
        return projectName;
    }

    public static String getDeployWarPath() {
        if (warpath == null) {
            init();
            if (StringUtils.isNotBlank(warpath)) {
                warpath = decodeUTF8(warpath);
                if ((!warpath.endsWith("/")) && (!warpath.endsWith(File.separator))) {
                    warpath += File.separatorChar;
                }
            }
        }
        return warpath;
    }

    public static String getLibPath() {
        String libPath = null;
        URL classPathUrl = ClassPathUtil.class.getResource("/");
        if (classPathUrl == null) {
            libPath = getPath() + "lib/";
        }
        else {
            File f = new File(classPathUrl.getFile()).getParentFile();
            if (f.exists()) {
                f.mkdirs();
            }
            String path = f.getPath() + File.separator + "lib" + File.separator;
            libPath = dealWithJboss(path, true);
        }
        if (StringUtils.isNotBlank(libPath)) {
            libPath = decodeUTF8(libPath);
        }
        log.debug("lib package path---------------->" + libPath
                + "#############your entity package must put in this path");
        return libPath;
    }

    public static String getClassesPath() {
        String classesPath = null;
        URL classPathUrl = ClassPathUtil.class.getResource("/");
        if (classPathUrl == null) {
            classesPath = getPath() + "bin/";
        }
        else {
            String path = classPathUrl.getFile();
            if ((classPathUrl.toString().indexOf("file:") != -1) && (path.startsWith("/"))) {
                path = path.substring(1);
            }
            if ((path.indexOf("target/classes/") < 0) && (path.indexOf("target/test-classes/") < 0)) {
                classesPath = dealWithJboss(path, false);
            }
            else {
                classesPath = path;
            }
        }
        if (StringUtils.isNotBlank(classesPath)) {
            classesPath = decodeUTF8(classesPath);
        }
        log.debug("classes path---------------->" + classesPath
                + "#############your classes files must put in this path");
        return classesPath;
    }

    private static void init() {
        if (StringUtils.isBlank(warpath)) {
            String clsPath = getClassesPath();
            if ((StringUtils.isNotBlank(clsPath)) && ((clsPath.indexOf("target/classes/") != -1)
                    || (clsPath.indexOf("target/test-classes/") != -1))) {
                if (clsPath.indexOf("target/classes/") != -1) {
                    warpath = clsPath.substring(0, clsPath.indexOf("target/classes/")) + "src/main/webapp/";
                }
                else {
                    warpath = clsPath.substring(0, clsPath.indexOf("target/test-classes/")) + "src/main/webapp/";
                }
            }
            else {
                URL url = ClassPathUtil.class.getResource(ClassPathUtil.class.getSimpleName() + ".class");
                String classPath = url.toString();
                int startIndex = classPath.indexOf("file:");
                int index = classPath.indexOf("WEB-INF");
                if (index >= 0) {
                    String deployWarPath = classPath.substring(startIndex + "file:".length() + 1, index);
                    warpath = deployWarPath;
                    if ((System.getProperty("os.name").indexOf("Linux") != -1)
                            && (!deployWarPath.startsWith(File.separator))) {
                        deployWarPath = File.separator + deployWarPath;
                    }
                }
            }
        }
    }

    private static String dealWithJboss(String path, boolean lib) {
        if (StringUtils.isNotBlank(warpath)) {
            path = warpath;
            if (lib) {
                path = path + "WEB-INF" + File.separator + "lib" + File.separator;
            }
            else {
                path = path + "WEB-INF" + File.separator + "classes" + File.separator;
            }
        }
        else if ((lib) && (path.endsWith("bin"))) {
            int index = path.lastIndexOf("bin");
            path = path.substring(0, index) + "lib" + File.separator;
        }
        return path;
    }

    private static String decodeUTF8(String path) {
        if (path.indexOf("%") != -1) {
            try {
                path = URLDecoder.decode(path, "utf-8");
            }
            catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        String system = System.getProperty("os.name");
        if (StringUtils.isNotBlank(system)) {
            system = system.toLowerCase();
        }
        if ((system.indexOf("linux") != -1) || (system.indexOf("mac os x") != -1)) {
            if ((!path.startsWith(File.separator)) && (!path.startsWith("/"))) {
                path = "/" + path;
            }
        }
        return path;
    }

    private static String getPath() {
        String result = null;
        URL url = ClassPathUtil.class.getResource(ClassPathUtil.class.getSimpleName() + ".class");
        String classPath = url.toString();
        int startIndex = classPath.indexOf("file:");
        int index = classPath.indexOf("/lib/");
        if (index >= 0) {
            result = classPath.substring(startIndex + "file:".length(), index) + "/";
        }
        return result;
    }
}
