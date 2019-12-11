package com.epoint.ztb.bigdata.tagmg.common;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.log4j.Logger;

public class PkgScanner
{
    private Logger logger = Logger.getLogger(getClass());

    /**
     * 包名
     */
    private String pkgName;

    /**
     * 包对应的路径名
     */
    private String pkgPath;

    private ClassLoader cl;

    public PkgScanner(String pkgName) {
        this.pkgName = pkgName;
        this.pkgPath = PathUtils.packageToPath(pkgName);

        cl = Thread.currentThread().getContextClassLoader();
    }

    /**
     * 执行扫描操作.
     *
     * @return
     * @throws IOException
     */
    public List<String> scan() {
        return scan(null);
    }

    /**
     * 执行扫描操作.
     *
     * @return
     * @throws IOException
     */
    public List<String> scan(Class<? extends Annotation> annotationclass) {
        List<String> list = loadResource();
        if (annotationclass != null) {
            list = filterComponents(list, annotationclass);
        }
        return list;
    }

    private List<String> loadResource() {
        List<String> list = null;

        try {
            Enumeration<URL> urls = cl.getResources(pkgPath);
            while (urls.hasMoreElements()) {
                URL u = urls.nextElement();
                ResourceType type = determineType(u);

                switch (type) {
                    case JAR:
                        String path = PathUtils.distillPathFromJarURL(u.getPath());
                        list = scanJar(path);
                        break;
                    case FILE:
                        list = scanFile(u.getPath(), pkgName);
                        break;
                    default:
                        break;
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * 根据URL判断是JAR包还是文件目录
     * 
     * @param url
     * @return
     */
    private ResourceType determineType(URL url) {
        if (url.getProtocol().equals(ResourceType.FILE.getTypeString())) {
            return ResourceType.FILE;
        }

        if (url.getProtocol().equals(ResourceType.JAR.getTypeString())) {
            return ResourceType.JAR;
        }

        throw new IllegalArgumentException("不支持该类型:" + url.getProtocol());
    }

    /**
     * 扫描JAR文件
     * 
     * @param path
     * @return
     * @throws IOException
     */
    private List<String> scanJar(String path) {
        List<String> classNameList = new ArrayList<>();
        JarFile jar = null;
        try {
            jar = new JarFile(path);
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();

                if ((name.startsWith(pkgPath)) && (name.endsWith(ResourceType.CLASS_FILE.getTypeString()))) {
                    name = PathUtils.trimSuffix(name);
                    name = PathUtils.pathToPackage(name);

                    classNameList.add(name);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (jar != null) {
                try {
                    jar.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return classNameList;
    }

    /**
     * 扫描文件目录下的类
     * 
     * @param path
     * @return
     */
    private List<String> scanFile(String path, String basePkg) {
        File f = new File(path);

        List<String> classNameList = new ArrayList<>(10);

        // 得到目录下所有文件(目录)
        File[] files = f.listFiles();
        if (null != files) {
            int LEN = files.length;

            for (int ix = 0; ix < LEN; ++ix) {
                File file = files[ix];

                // 判断是否还是一个目录
                if (file.isDirectory()) {
                    // 递归遍历目录
                    List<String> list = scanFile(file.getAbsolutePath(),
                            PathUtils.concat(basePkg, ".", file.getName()));
                    classNameList.addAll(list);

                }
                else if (file.getName().endsWith(ResourceType.CLASS_FILE.getTypeString())) {
                    // 如果是以.class结尾
                    String className = PathUtils.trimSuffix(file.getName());
                    // 如果类名中有"$"不计算在内
                    if (-1 != className.lastIndexOf("$")) {
                        continue;
                    }

                    // 命中
                    String result = PathUtils.concat(basePkg, ".", className);
                    classNameList.add(result);
                }
            }
        }

        return classNameList;
    }

    /**
     * 过虑掉没有指定注解的类
     * 
     * @param classList
     * @return
     */
    private List<String> filterComponents(List<String> classList, Class<? extends Annotation> annotationclass) {
        List<String> newList = new ArrayList<String>();
        classList.forEach(name -> {
            try {
                Class<?> clazz = Class.forName(name);
                Annotation an = clazz.getAnnotation(annotationclass);
                if (null != an) {
                    newList.add(name);
                }
            }
            catch (Exception e) {
                logger.error(name, e);
            }
        });

        return newList;
    }
}

class PathUtils
{
    private PathUtils() {
    }

    /**
     * 把路径字符串转换为包名.
     * a/b/c/d -> a.b.c.d
     *
     * @param path
     * @return
     */
    public static String pathToPackage(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        return path.replace("/", ".");
    }

    /**
     * 包名转换为路径名
     * 
     * @param pkg
     * @return
     */
    public static String packageToPath(String pkg) {
        return pkg.replace(".", "/");
    }

    /**
     * 将多个对象转换成字符串并连接起来
     * 
     * @param objs
     * @return
     */
    public static String concat(Object... objs) {
        StringBuilder sb = new StringBuilder(30);
        for (int ix = 0; ix < objs.length; ++ix) {
            sb.append(objs[ix]);
        }

        return sb.toString();
    }

    /**
     * 去掉文件的后缀名
     * 
     * @param name
     * @return
     */
    public static String trimSuffix(String name) {
        int dotIndex = name.indexOf('.');
        if (-1 == dotIndex) {
            return name;
        }

        return name.substring(0, dotIndex);
    }

    public static String distillPathFromJarURL(String url) {
        int startPos = url.indexOf(':');
        int endPos = url.lastIndexOf('!');

        return url.substring(startPos + 1, endPos);
    }
}

enum ResourceType
{
    JAR("jar"), FILE("file"),

    CLASS_FILE(".class");

    private String typeString;

    private ResourceType(String type) {
        this.typeString = type;
    }

    public String getTypeString() {
        return this.typeString;
    }
}
