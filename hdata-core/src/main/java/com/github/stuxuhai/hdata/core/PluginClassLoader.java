package com.github.stuxuhai.hdata.core;

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PluginClassLoader extends URLClassLoader {
    private static final Logger LOGGER = LogManager.getLogger();

    public PluginClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        ClassLoader cl = getClass().getClassLoader();
        LOGGER.info(name + " => pluginClassLoader.getClass().getClassLoader() : " + cl);
        if (name.startsWith("com.github.stuxuhai.hdata.api.") || name.startsWith("org.apache.logging.") || name.startsWith("org.apache.log4j.")
                || name.startsWith("org.slf4j.") || name.startsWith("org.apache.commons.logging.")) {
            return cl.loadClass(name);
        } else {
            LOGGER.info(name + " => pluginClassLoader.super : " + super.loadClass(name).getClassLoader());
            return super.loadClass(name);
        }
    }

}
