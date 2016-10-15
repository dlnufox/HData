package com.github.stuxuhai.hdata.api;

public abstract class Reader extends AbstractPlugin {

	public void prepare(JobContext context, PluginConfig readerConfig) {
	}

	/**
	 * 执行读取数据过程，并通过 RecordCollector 实例向 Disruptor 发布事件
	 * @param recordCollector
     */
	public void execute(RecordCollector recordCollector) {
	}

	public void close() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public abstract Splitter newSplitter();
}
