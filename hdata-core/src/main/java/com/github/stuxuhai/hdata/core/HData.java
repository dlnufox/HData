package com.github.stuxuhai.hdata.core;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.JobStatus;
import com.github.stuxuhai.hdata.api.Metric;
import com.github.stuxuhai.hdata.api.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.common.HDataConfigConstants;
import com.github.stuxuhai.hdata.config.DefaultEngineConfig;
import com.github.stuxuhai.hdata.config.DefaultJobConfig;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Throwables;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 *【HData设计】
 * 配置文件：XML格式，配置Reader、Writer的参数（如：并行度、数据库连接地址、账号、密码等）；
 * Reader：数据读取模块，负责从数据源读取数据并写入RingBuffer；
 * Splitter：根据配置文件中Reader的并行度构造相应数据的ReaderConfig对象供Reader使用，以实现数据的并行读取；
 * RingBuffer：来自Disruptor的高性能环形数据缓冲区，基于事件监听模式的异步实现，采用无锁方式针对CPU缓存优化，在此用于Reader和Writer的数据交换；
 * Writer：数据写入模块，负责从RingBuffer中读取数据并写入目标数据源。
 *
 * HData框架通过配置读取解析、RingBuffer 缓冲区、线程池封装等技术，统一处理了数据传输中的基本问题，并提供Reader、Splitter、Writer插件接口，
 * 基于此可以方便地开发出各种插件，以满足各种数据源访问的需求。
 */
public class HData {

    private int exitCode = 0;
    private static final DecimalFormat decimalFormat = new DecimalFormat("#0.00");
    private static final Logger LOGGER = LogManager.getLogger(HData.class);

    public void start(final DefaultJobConfig jobConfig) {
        final PluginConfig readerConfig = jobConfig.getReaderConfig();
        final PluginConfig writerConfig = jobConfig.getWriterConfig();

        /**
         * 读数的包名，如 jdbc
         */
        String readerName = jobConfig.getReaderName();
        /**
         * 写入的包名，如 hive
         */
        String writerName = jobConfig.getWriterName();

        LOGGER.info("Reader: {}, Writer: {}", readerName, writerName);

        final JobContext context = new JobContext();
        context.setJobConfig(jobConfig);

        final Metric metric = new Metric();
        context.setMetric(metric);

        final OutputFieldsDeclarer outputFieldsDeclarer = new OutputFieldsDeclarer();
        context.setDeclarer(outputFieldsDeclarer);

        final DefaultEngineConfig engineConfig = DefaultEngineConfig.create();
        context.setEngineConfig(engineConfig);

        long sleepMillis = engineConfig.getLong(HDataConfigConstants.HDATA_SLEEP_MILLIS, Constants.DEFAULT_HDATA_SLEEP_MILLIS);

        List<PluginConfig> readerConfigList = null;

        /**
         * 获取对应的Reader
         */
        final Splitter splitter = jobConfig.newSplitter();

        if (splitter != null) {
            LOGGER.info("Executing splitter for reader.");

            ExecutorService es = Executors.newCachedThreadPool();
            Callable<List<PluginConfig>> callable = new Callable<List<PluginConfig>>() {  //根据List<PluginConfig>>来决定reader的并发度
                @Override
                public List<PluginConfig> call() throws Exception {
                    /**
                     * 把 reader 的类加载器设置为当前线程的上下文的ClassLoader
                     */
                    Thread.currentThread().setContextClassLoader(splitter.getClass().getClassLoader());
                    return splitter.split(jobConfig);
                }
            };

            Future<List<PluginConfig>> future = es.submit(callable);
            es.shutdown();
            try {
                readerConfigList = future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("", e);
                System.exit(JobStatus.FAILED.getStatus());
            }

            if (readerConfigList.isEmpty()) {
                System.exit(JobStatus.SUCCESS.getStatus());
            }
        } else {
            if (readerConfig.getParallelism() > 1) {
                LOGGER.warn("Can not find splitter, reader parallelism is set to 1.");
            }
            readerConfigList = new ArrayList<>();
            readerConfigList.add(readerConfig);
        }

        Reader[] readers = new Reader[readerConfigList.size()];
        for (int i = 0, len = readers.length; i < len; i++) {
            readers[i] = jobConfig.newReader();
        }

        context.setReaders(readers);

        /**
         * writer 的并发度
         */
        int writerParallelism = writerConfig.getParallelism();
        LOGGER.info("Reader parallelism: {}, Writer parallelism: {}", readers.length, writerParallelism);

        final Writer[] writers = new Writer[writerParallelism];

        /**
         * 实例化事件 handlers
         */
        final RecordWorkHandler[] handlers = new RecordWorkHandler[writerParallelism];
        for (int i = 0; i < writerParallelism; i++) {
            writers[i] = jobConfig.newWriter();
            /**
             * 初始化数据写入 handler ，一个 writer 对应一个 handler
             */
            handlers[i] = new RecordWorkHandler(writers[i], context, writerConfig);
        }
        context.setWriters(writers);

        /**
         * disruptor 的队列长度
         */
        int bufferSize = engineConfig.getInt(HDataConfigConstants.STORAGE_BUFFER_SIZE, 1024);

        /**
         * disruptor 的等待策略
         */
        String WaitStrategyName = engineConfig.getString(HDataConfigConstants.HDATA_STORAGE_DISRUPTOR_WAIT_STRATEGY,
                BlockingWaitStrategy.class.getName());

        /**
         * 初始化一个事件处理器
         * 其中 handlers 都是 Disruptor 的 WorkerHandler 类的实例，handlers 用于注册到事件队列 disruptor
         */
        DefaultStorage storage = createStorage(bufferSize, WaitStrategyName, readers.length, handlers, context);

        context.setStorage(storage);

        LOGGER.info("Transfer data from reader to writer...");

        /**
         * 定义数据处理器，并作为 ReaderWorker 的参数
         * 其中参数 storage 中包含一个所有读线程共用的 disruptor
         */
        DefaultRecordCollector rc = new DefaultRecordCollector(storage, metric, readerConfig.getFlowLimit());


        ExecutorService es = Executors.newFixedThreadPool(readers.length);
        CompletionService<Integer> cs = new ExecutorCompletionService<>(es);

        for (int i = 0, len = readerConfigList.size(); i < len; i++) {
            /**
             * 实例化读数线程，每个线程都使用同一个 DefaultRecordCollector 实例 rc，以达到所有读线程共用一个 disruptor 目的
             */
            ReaderWorker readerWorker = new ReaderWorker(readers[i], context, readerConfigList.get(i), rc);

            /**
             * 提交抽数线程到执行队列中
             */
            cs.submit(readerWorker);
        }
        es.shutdown();

        metric.setReaderStartTime(System.currentTimeMillis());
        metric.setWriterStartTime(System.currentTimeMillis());
        while (!es.isTerminated()) {
            if (context.isWriterError()) {
                LOGGER.info("Write error.");
                LOGGER.info("Closing reader and writer.");
                // storage.close();
                closeReaders(readers);
                closeWriters(writers);
                LOGGER.info("Job run failed!");
                System.exit(JobStatus.FAILED.getStatus());
            }

            Utils.sleep(sleepMillis);
            LOGGER.info("Read: {}\tWrite: {}", metric.getReadCount().get(), metric.getWriteCount().get());
        }

        context.setReaderFinished(true);
        metric.setReaderEndTime(System.currentTimeMillis());

        while (!storage.isEmpty()) {
            if (context.isWriterError()) {
                LOGGER.info("Write error.");
                closeWriters(writers);
                LOGGER.info("Job run failed!");
                System.exit(JobStatus.FAILED.getStatus());
            }
            Utils.sleep(sleepMillis);

            LOGGER.info("Read Finished(total: {}), Write: {}", metric.getReadCount().get(), metric.getWriteCount().get());
        }

        storage.close();
        LOGGER.info("Read Finished(total: {}), Write Finished(total: {})", metric.getReadCount().get(), metric.getWriteCount().get());

        for (int i = 0, len = readers.length; i < len; i++) {
            try {
                Future<Integer> future = cs.take();
                if (future == null) {
                    LOGGER.info("Read error.");
                    closeWriters(writers);
                    LOGGER.info("Job run failed!");
                    System.exit(1);
                }

                Integer result = future.get();
                if (result == null) {
                    result = -1;
                }

                if (result != 0) {
                    LOGGER.info("Read error.");
                    closeWriters(writers);
                    LOGGER.info("Job run failed!");
                    System.exit(result);
                }
            } catch (Exception e) {
                LOGGER.error(Throwables.getStackTraceAsString(e));
                exitCode = 1;
            }
        }

        metric.setWriterEndTime(System.currentTimeMillis());
        if (!closeWriters(writers)) {
            exitCode = 1;
        }

        context.setWriterFinished(true);

        double readSeconds = (metric.getReaderEndTime() - metric.getReaderStartTime()) / 1000d;
        double writeSeconds = (metric.getWriterEndTime() - metric.getWriterStartTime()) / 1000d;
        String readSpeed = decimalFormat.format(metric.getReadCount().get() / readSeconds);
        String writeSpeed = decimalFormat.format(metric.getWriteCount().get() / writeSeconds);
        LOGGER.info("Read spent time: {}s, Write spent time: {}s", decimalFormat.format(readSeconds), decimalFormat.format(writeSeconds));
        LOGGER.info("Read records: {}/s, Write records: {}/s", readSpeed, writeSpeed);

        // PluginUtils.closeURLClassLoader();

        System.exit(exitCode);
    }

    private DefaultStorage createStorage(int bufferSize, String WaitStrategyName, int producerCount, RecordWorkHandler[] handlers,
            JobContext context) {
        WaitStrategy waitStrategy = WaitStrategyFactory.build(WaitStrategyName);
        ProducerType producerType;
        if (producerCount == 1) {
            producerType = ProducerType.SINGLE;
        } else {
            producerType = ProducerType.MULTI;
        }
        Disruptor<RecordEvent> disruptor = new Disruptor<RecordEvent>(RecordEvent.FACTORY, bufferSize, Executors.defaultThreadFactory(), producerType,
                waitStrategy);
        DefaultStorage storage = new DefaultStorage(disruptor, handlers, context);
        return storage;
    }

    private boolean closeReaders(final Reader[] readers) {
        ExecutorService es = Executors.newCachedThreadPool();
        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.currentThread().setContextClassLoader(readers[0].getClass().getClassLoader());
                try {
                    for (Reader reader : readers) {
                        reader.close();
                    }

                    return true;
                } catch (Exception e) {
                    LOGGER.error(Throwables.getStackTraceAsString(e));
                }
                return false;
            }
        };

        Future<Boolean> future = es.submit(callable);
        es.shutdown();

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            return false;
        }
    }

    private boolean closeWriters(final Writer[] writers) {
        ExecutorService es = Executors.newCachedThreadPool();
        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.currentThread().setContextClassLoader(writers[0].getClass().getClassLoader());
                try {
                    for (Writer writer : writers) {
                        writer.close();
                    }

                    return true;
                } catch (Exception e) {
                    LOGGER.error(Throwables.getStackTraceAsString(e));
                }
                return false;
            }
        };

        Future<Boolean> future = es.submit(callable);
        es.shutdown();

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            return false;
        }
    }
}
