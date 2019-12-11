package com.epoint.ztb.bigdata.tagmg.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * 通用工具
 * 
 * @author hjliang
 * @version 2019年5月23日
 */
public class Functions
{
    private final static Logger logger = Logger.getLogger(Functions.class);

    public static String TrimEnd(String str, String ch) {
        if (null == str) {
            return "";
        }
        if (!str.endsWith(ch))
            return str;
        return TrimEnd(str.substring(0, str.length() - ch.length()), ch);
    }

    public static String TrimStart(String str, String ch) {
        if (null == str) {
            return "";
        }
        if (!str.startsWith(ch))
            return str;

        return TrimStart(str.substring(ch.length()), ch);
    }

    public static String Trim(String str, String ch) {
        if (null == str) {
            return "";
        }
        return TrimEnd(TrimStart(str, ch), ch);
    }

    public static String getProjectPath() {
        return new File("").getAbsolutePath();
    }

    public static String getClassPath() {
        return Functions.class.getResource("/").getPath();
    }

    public static boolean cmd(String... cmds) {
        boolean isSuccess = true;
        try {
            Runtime runtime = Runtime.getRuntime();
            for (String cmd : cmds) {
                Process proc = runtime.exec(cmd);
                InputStreamReader isr = new InputStreamReader(proc.getInputStream(), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null) {
                    logger.info(line.toString());
                }
                proc.waitFor();

                if (proc.exitValue() != 0) {
                    isSuccess = false;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            isSuccess = false;
        }
        return isSuccess;
    }

    public static String convertDate2String(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    public static String getGridSort(String sql, String sortField, String sortOrder, String defaultSort) {
        if (StringUtils.isBlank(sortField)) {
            // 默认搜索条件后台写死的不存在SQL注入
            return " " + defaultSort;
        }
        else {
            String sortStr = " ORDER BY " + sortField + " " + sortOrder;
            // String sortStr = " ORDER BY " + sortField + " " + sortOrder;
            // 解决表格分页数据重复显示的问题，通过拼接默认排序的方式保证排序结果唯一 张佩龙 2018-01-15
            if (StringUtils.isNotBlank(defaultSort)) {
                // BY FJX 这里也是处理默认order by 后台写死 不存在SQL注入
                sortStr += "," + Pattern.compile("order\\s*by", Pattern.CASE_INSENSITIVE).matcher(defaultSort)
                        .replaceFirst("");
            }
            return sortStr;
        }
    }

    public static void updateJarFile(File srcJarFile, File... filesToAdd) throws IOException {

        File tmpJarFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp");
        JarFile jarFile = new JarFile(srcJarFile);
        boolean jarUpdated = false;
        List<String> fileNames = new ArrayList<String>();

        try {
            JarOutputStream tempJarOutputStream = new JarOutputStream(new FileOutputStream(tmpJarFile));

            try {
                for (int i = 0; i < filesToAdd.length; i++) {
                    File file = filesToAdd[i];
                    FileInputStream fis = new FileInputStream(file);
                    try {
                        byte[] buffer = new byte[1024];
                        int bytesRead = 0;
                        JarEntry entry = new JarEntry(file.getName());
                        fileNames.add(entry.getName());
                        tempJarOutputStream.putNextEntry(entry);
                        while ((bytesRead = fis.read(buffer)) != -1) {
                            tempJarOutputStream.write(buffer, 0, bytesRead);
                        }
                    }
                    finally {
                        fis.close();
                    }
                }

                Enumeration<?> jarEntries = jarFile.entries();
                while (jarEntries.hasMoreElements()) {
                    JarEntry entry = (JarEntry) jarEntries.nextElement();

                    String[] fileNameArray = (String[]) fileNames.toArray(new String[0]);
                    Arrays.sort(fileNameArray);
                    if (Arrays.binarySearch(fileNameArray, entry.getName()) < 0) {
                        InputStream entryInputStream = jarFile.getInputStream(entry);
                        tempJarOutputStream.putNextEntry(entry);
                        byte[] buffer = new byte[1024];
                        int bytesRead = 0;
                        while ((bytesRead = entryInputStream.read(buffer)) != -1) {
                            tempJarOutputStream.write(buffer, 0, bytesRead);
                        }
                    }
                }

                jarUpdated = true;
            }
            catch (Exception e) {
                e.printStackTrace();
                tempJarOutputStream.putNextEntry(new JarEntry("stub"));
            }
            finally {
                tempJarOutputStream.close();
            }

        }
        finally {
            jarFile.close();

            if (!jarUpdated) {
                tmpJarFile.delete();
            }
        }

        if (jarUpdated) {
            srcJarFile.delete();
            tmpJarFile.renameTo(srcJarFile);
        }
    }

    public static boolean existsInJar(File srcJarFile, String regex) {
        JarInputStream in = null;
        try {
            in = new JarInputStream(new FileInputStream(srcJarFile));
            ZipEntry entry = null;
            while ((entry = in.getNextEntry()) != null) {
                if (entry.getName().matches(regex)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public static List<File> listFiles(File file, List<String> keywords) {
        List<File> filelist = new LinkedList<File>();
        if (file.isDirectory()) {
            listFiles(file, filelist, keywords);
        }
        else {
            if (keywords == null || keywords.isEmpty()) {
                filelist.add(file);
            }
            else {
                for (String keyword : keywords) {
                    if (file.getPath().contains(keyword)) {
                        filelist.add(file);
                        break;
                    }
                }
            }
        }
        return filelist;
    }

    private static void listFiles(File file, List<File> filelist, List<String> keywords) {
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                listFiles(f, filelist, keywords);
            }
            else {
                if (keywords == null || keywords.isEmpty()) {
                    filelist.add(f);
                }
                else {
                    for (String keyword : keywords) {
                        if (f.getPath().contains(keyword)) {
                            filelist.add(f);
                            break;
                        }
                    }
                }
            }
        }
    }
}
