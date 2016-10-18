package com.github.stuxuhai.hdata.core;

import java.util.concurrent.Callable;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;

public class ReaderWorker implements Callable<Integer> {

    private final JobContext context;
    private final DefaultRecordCollector rc;

    /**
     * 每个 ReaderWorker 对应一份 reader
     */
    private final Reader reader;

    /**
     * 每个 ReaderWorker 对应一份 readerConfig，当 ReaderWorker 调用 reader.prepare() 时作为参数传递给 Reader
     */
    private final PluginConfig readerConfig;

    /**
     * 实例化用于完成读取数及发布 disruptor 事件的线程
     * Reader 与 ReaderWorker 一一对应
     * 参数 rc 在 reader.execute() 中使用，通过 RecordCollector.send() 向 disruptor 发布事件
     * @param reader
     * @param context
     * @param readerConfig
     * @param rc            该参数在 reader.execute() 中使用，通过 RecordCollector.send() 向 disruptor 发布事件
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

        /**
         * 执行任务前，设置该子任务的 readerConfig
         */
        reader.prepare(context, readerConfig);

        /**
         * 执行各个类型 reader 中的 execute 方法读取数据
         * 再通过 recordCollector.send() 向事件队列发布事件
         */
        reader.execute(rc);
        reader.close();
        return 0;
    }

}
