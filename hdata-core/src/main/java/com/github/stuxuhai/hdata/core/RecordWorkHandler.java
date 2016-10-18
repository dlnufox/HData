package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Metric;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Writer;
import com.lmax.disruptor.WorkHandler;

/**
 * 每个 Handler 对应一个 Processor 实例
 */
public class RecordWorkHandler implements WorkHandler<RecordEvent> {

    /**
     * 每个 Handler 对应一个 writer
     */
    private final Writer writer;
    private final JobContext context;

    /**
     * 每个 Handler 对应一份 writerConfig
     */
    private final PluginConfig writerConfig;
    private boolean writerPrepared;
    private final Metric metric;

    /**
     * 初始化数据写入 Handler 实例
     * @param writer
     * @param context
     * @param writerConfig
     */
    public RecordWorkHandler(Writer writer, JobContext context, PluginConfig writerConfig) {
        this.writer = writer;
        this.context = context;
        this.writerConfig = writerConfig;
        this.metric = context.getMetric();
    }

    /**
     * disruptor 的事件回调方法，当有事件触发时自动调用 onEvent 方法
     * @param event
     */
    @Override
    public void onEvent(RecordEvent event) {
        // preparing for writing
        if (!writerPrepared) {

            /**
             * 声名抽数的字段
             */
            context.declareOutputFields();
            Thread.currentThread().setContextClassLoader(writer.getClass().getClassLoader());

            /**
             * 把 writerConfig 作为参数传递给 writer
             */
            writer.prepare(context, writerConfig);
            writerPrepared = true;
            if (metric.getWriterStartTime() == 0) {
                metric.setWriterStartTime(System.currentTimeMillis());
            }
        }

        /**
         * 调用自定义的数据处理过程，写入对应的数据库，包括文件系统、console等
         */
        writer.execute(event.getRecord());
        metric.getWriteCount().incrementAndGet();
    }
}
