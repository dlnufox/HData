package com.github.stuxuhai.hdata.core;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.github.stuxuhai.hdata.api.Metric;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Stopwatch;

/**
 * 数据处理器
 * 定义了 send() 方法用于向 Disruptor 事件队列发布事件
 */
public class DefaultRecordCollector implements RecordCollector {

    private static final Logger LOGGER = LogManager.getLogger(DefaultRecordCollector.class);

    private static final long SLEEP_MILL_SECONDS = 1000;

    private final DefaultStorage storage;
    private final Metric metric;
    private final long flowLimit;
    private Stopwatch stopwatch = Stopwatch.createStarted();

    /**
     * 实例化数据处理器，并传入到 ReaderWorker 线程中
     * 当线程的 call 方法被执行时，该实例作为 reader.execute(rc) 的参数使用
     * 最后在 reader.execute(rc) 方法内部调用 RecordCollector.send() 向 disruptor 发布事件
     * @param storage
     * @param metric
     * @param flowLimit
     */
    public DefaultRecordCollector(DefaultStorage storage, Metric metric, long flowLimit) {
        this.storage = storage;
        this.metric = metric;
        this.flowLimit = flowLimit;
        LOGGER.info("The flow limit is {} bytes/s.", this.flowLimit);
    }

    /**
     * 发布一个事件，触发事件处理机制
     */
    @Override
    public void send(Record record) {
        // 限速
        if (flowLimit > 0) {
            while (true) {
                long currentSpeed = metric.getSpeed();
                if (currentSpeed > flowLimit) {
                    if (stopwatch.elapsed(TimeUnit.SECONDS) >= 5) {
                        LOGGER.info("Current Speed is {} MB/s, sleeping...", String.format("%.2f", (double) currentSpeed / 1024 / 1024));
                        stopwatch.reset();
                    }
                    Utils.sleep(SLEEP_MILL_SECONDS);
                } else {
                    break;
                }
            }
        }

        /**
         * 发布一个事件，触发事件处理机制
         */
        storage.put(record);
        metric.getReadCount().incrementAndGet();

        if (flowLimit > 0) {
            metric.getReadBytes().addAndGet(RamUsageEstimator.sizeOf(record));
        }

    }

    /**
     * 这个方法可以做到限速吗？
     * @param records
     */
    @Override
    public void send(Record[] records) {
        storage.put(records);
    }
}
