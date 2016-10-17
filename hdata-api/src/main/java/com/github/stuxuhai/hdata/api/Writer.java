package com.github.stuxuhai.hdata.api;

public abstract class Writer extends AbstractPlugin {

	public void prepare(JobContext context, PluginConfig writerConfig) {
	}

	/**
	 * 执行写入前处理事件、写入数据的过程，每个事件都会触发一次execute方法
	 * @param record
     */
	public void execute(Record record) {
	}

	public void close() {
	}
}
