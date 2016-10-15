package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Storage;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DefaultStorage implements Storage {

    /**
     * 事件队列
     */
    private final Disruptor<RecordEvent> disruptor;
    private final RingBuffer<RecordEvent> ringBuffer;

    private static final EventTranslatorOneArg<RecordEvent, Record> TRANSLATOR = new EventTranslatorOneArg<RecordEvent, Record>() {

        @Override
        public void translateTo(RecordEvent event, long sequence, Record record) {
            event.setRecord(record);
        }
    };

    /**
     * 初始化事件处理类，作用 disruptor 作为事件队列
     * @param disruptor
     * @param handlers
     * @param context
     */
    public DefaultStorage(Disruptor<RecordEvent> disruptor, RecordWorkHandler[] handlers, JobContext context) {
        this.disruptor = disruptor;
        disruptor.setDefaultExceptionHandler(new RecordEventExceptionHandler(disruptor, context));

        /**
         * 向 disruptor 中注册事件处理方法
         */
        disruptor.handleEventsWithWorkerPool(handlers);
        /**
         * 启动消费进程
         */
        ringBuffer = disruptor.start();
    }

    /**
     * 发布事件，触发事件处理机制
     * @param record
     */
    @Override
    public void put(Record record) {
        disruptor.publishEvent(TRANSLATOR, record);
    }

    /**
     * 发布一组事件
     * @param records
     */
    @Override
    public void put(Record[] records) {
        for (Record record : records) {
            put(record);
        }
    }

    /**
     * 用于检测所有数据是否都已写入数据库
     * @return
     */
    @Override
    public boolean isEmpty() {
        return ringBuffer.remainingCapacity() == ringBuffer.getBufferSize();
    }

    @Override
    public int size() {
        return ringBuffer.getBufferSize();
    }

    @Override
    public void close() {
        disruptor.shutdown();
    }
}
