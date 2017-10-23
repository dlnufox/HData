package com.github.stuxuhai.hdata.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.core.PluginClassLoader;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PluginUtils {

    /**
     * 用于记录每个插件对应的 ClassLoader
     */
    private static Map<String, PluginClassLoader> cache = Maps.newConcurrentMap();
    private static final Logger LOGGER = LogManager.getLogger();

    private static List<URL> listFileByPluginName(String pluginName) throws MalformedURLException {
        List<URL> result = Lists.newArrayList();
        /**
         * 得到 hdata-core-${version}.jar 所在的目录
         */
        String path = PluginUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        LOGGER.info("name of core jar:" + path);
        LOGGER.info("path of core jar" + "/home/xiaochaihu/projects/tools/hdata/lib/hdata-core-0.2.8.jar".replaceAll("/lib/.*\\.jar", ""));

        /**
         * 得到对应 reader 对应模块的jar包，及其所依赖的所有包
         */
        File file = new File(path.replaceAll("/lib/.*\\.jar", "") + "/plugins/" + pluginName);
        if (!file.exists()) {
            throw new HDataException("Plugin not found: " + pluginName);
        }

        /**
         * 得到所有jar包的路径
         */
        File[] jars = file.listFiles();
        for (File jar : jars) {
            result.add(jar.toURI().toURL());
        }
        return result;
    }

    public static Class<?> loadClass(String pluginName, String className) throws ClassNotFoundException, MalformedURLException {
        /**
         * 根据插件名称，从缓存中获取对应的 ClassLoader，以便于隔离依赖，防止依赖冲突
         */
        PluginClassLoader classLoader = cache.get(pluginName);

        /**
         * 如果缓存中没有对应的 ClassLoader，则用之前得到的插件依赖，生成一个并放在缓存中，方便后续使用
         */
        if (classLoader == null) {
            /**
             * 根据插件名称，获取插件及其所有依赖包的绝对路径
             */
            List<URL> list = listFileByPluginName(pluginName);
            classLoader = new PluginClassLoader(list.toArray(new URL[list.size()]));
            cache.put(pluginName, classLoader);
        }

        /**
         * 根据插件对应的类名，得到一个该 reader 的实例
         */
        return classLoader.loadClass(className);
    }

    public static void closeURLClassLoader() {
        for (Entry<String, PluginClassLoader> entry : cache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }
    }

    public static void main(String [] args) {
        System.out.println("/home/xiaochaihu/projects/tools/hdata/lib/hdata-core-0.2.8.jar".replaceAll("/lib/.*\\.jar", ""));
        try {
            System.out.println(PluginUtils.class.newInstance());
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
