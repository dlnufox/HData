package com.github.stuxuhai.hdata.api;

public abstract class Reader extends AbstractPlugin {

	public void prepare(JobContext context, PluginConfig readerConfig) {
	}

	/**
	 * execute()方法内部必须定义抽数的行为
	 * 在被 ReaderWorker 调用时读取数据，并通过 RecordCollector 实例向 Disruptor 发布事件
	 * @param recordCollector
     */
	public void execute(RecordCollector recordCollector) {
	}

	public void close() {
	}

	/**
	 * 声名输出字段
	 * @param declarer
     */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public abstract Splitter newSplitter();
}
