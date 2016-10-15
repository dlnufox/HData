package com.github.stuxuhai.hdata.core;

import java.util.concurrent.Callable;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;

public class ReaderWorker implements Callable<Integer> {

    private final Reader reader;
    private final JobContext context;
    private final DefaultRecordCollector rc;
    private final PluginConfig readerConfig;

    /**
     * 用于完成读取数及发布 disruptor 事件的线程
     * 一个 reader 对应一个线程
     * @param reader
     * @param context
     * @param readerConfig
     * @param rc
     */
    public ReaderWorker(Reader reader, JobContext context, PluginConfig readerConfig, DefaultRecordCollector rc) {
        this.reader = reader;
        this.context = context;
        this.rc = rc;
        this.readerConfig = readerConfig;
    }

    /**
     * ReaderWorker 继承了 Callable , 在线程池中会被调用 call 方法，从而执行读数据的过程
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws Exception {
        Thread.currentThread().setContextClassLoader(reader.getClass().getClassLoader());
        reader.prepare(context, readerConfig);

        /**
         * 执行各个类型 reader 中的 execute 方法读取数据，再通过 recordCollector 向事件队列发布事件
         */
        reader.execute(rc);
        reader.close();
        return 0;
    }

}
