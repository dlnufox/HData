package com.github.stuxuhai.hdata.plugin.reader.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class HBaseSplitter extends Splitter {

	private static final Logger LOGGER = LogManager.getLogger(HBaseSplitter.class);

	@Override
	public List<PluginConfig> split(JobConfig jobConfig) {
		List<PluginConfig> list = new ArrayList<>();
		PluginConfig readerConfig = jobConfig.getReaderConfig();

		/**
		 * 任务并发数
		 */
		int parallelism = readerConfig.getParallelism();

		/**
		 * 起始行键
		 */
		String startRowkey = readerConfig.getString(HBaseReaderProperties.START_ROWKWY, "");
		byte[] startRowkeyBytes = startRowkey.getBytes();

		/**
		 * 结束行键
		 */
		String endRowkey = readerConfig.getString(HBaseReaderProperties.END_ROWKWY, "");
		byte[] endRowkeyBytes = endRowkey.getBytes();

		if (parallelism == 1) {
			readerConfig.put(HBaseReaderProperties.START_ROWKWY, startRowkeyBytes);
			readerConfig.put(HBaseReaderProperties.END_ROWKWY, endRowkeyBytes);
			list.add(readerConfig);
			return list;
		} else {
			Configuration conf = HBaseConfiguration.create();
			if (readerConfig.containsKey(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT)) {
				conf.set(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT, readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT));
			}
			String zookeeperQuorum = readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_QUORUM);
			Preconditions.checkNotNull(zookeeperQuorum, "HBase reader required property: zookeeper.quorum");

			conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
			conf.set("hbase.zookeeper.property.clientPort", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
			try {
				Connection conn = ConnectionFactory.createConnection(conf);
				/**
				 * 获取表名，得到表的实例
				 */
				TableName tableName = TableName.valueOf(readerConfig.getString(HBaseReaderProperties.TABLE));
				Table table = conn.getTable(tableName);
				Preconditions.checkNotNull(table, "HBase reader required property: table");

				/**
				 * 得到表的region定位信息
				 */
				RegionLocator regionLocator = conn.getRegionLocator(tableName);

				/**
				 * Gets the starting and ending row keys for every region in the currently open table.
				 * This is mainly useful for the MapReduce integration.
				 */
				Pair<byte[][], byte[][]> startEndKeysPair = regionLocator.getStartEndKeys();
				table.close();

				// 根据 rowkey 区间切分抽数线程
				List<Pair<byte[], byte[]>> selectedPairList = new ArrayList<>();

				/**
				 * 第一行的 rowkey
				 */
				byte[][] startKeys = startEndKeysPair.getFirst();

				/**
				 * 最后一行的 rowkey
				 */
				byte[][] endKeys = startEndKeysPair.getSecond();

				if (startKeys.length == 1) {
					Pair<byte[], byte[]> pair = new Pair<>();
					pair.setFirst(startRowkeyBytes);
					pair.setSecond(endRowkeyBytes);
					selectedPairList.add(pair);
				} else {
					if (startRowkeyBytes.length == 0 && endRowkeyBytes.length == 0) {
						for (int i = 0, len = startKeys.length; i < len; i++) {
							Pair<byte[], byte[]> pair = new Pair<>();
							pair.setFirst(startKeys[i]);
							pair.setSecond(endKeys[i]);
							selectedPairList.add(pair);
						}
					} else if (endRowkeyBytes.length == 0) {
						for (int i = 0, len = startKeys.length; i < len; i++) {
							if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0) {
								Pair<byte[], byte[]> pair = new Pair<>();
								pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i]
										: startRowkeyBytes);
								pair.setSecond(endKeys[i]);
								selectedPairList.add(pair);
							}
						}
					} else {
						for (int i = 0, len = startKeys.length; i < len; i++) {
							if (len == 1) {
								Pair<byte[], byte[]> pair = new Pair<>();
								pair.setFirst(startRowkeyBytes);
								pair.setSecond(endRowkeyBytes);
								selectedPairList.add(pair);
								break;
							} else if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0
									&& Bytes.compareTo(endRowkeyBytes, startKeys[i]) >= 0) {
								Pair<byte[], byte[]> pair = new Pair<>();
								pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i]
										: startRowkeyBytes);
								pair.setSecond(
										Bytes.compareTo(endKeys[i], endRowkeyBytes) <= 0 ? endKeys[i] : endRowkeyBytes);
								selectedPairList.add(pair);
							}
						}
					}
				}

				if (parallelism > selectedPairList.size()) {
					LOGGER.info(
							"parallelism: {} is greater than the region count: {} in the currently open table: {}, so parallelism is set equal to region count.",
							parallelism, selectedPairList.size(), table.getName().getNameAsString());
					parallelism = selectedPairList.size();
				}

				/**
				 * HBase 抽数并行度的切分的关键
				 * 为什么 selectedPairList.size() 要定义为 double？
				 */
				double step = (double) selectedPairList.size() / parallelism;
				for (int i = 0; i < parallelism; i++) {
					List<Pair<byte[], byte[]>> splitedPairs = new ArrayList<>();
					for (int start = (int) Math.ceil(step * i), end = (int) Math .ceil(step * (i + 1)); start < end; start++) {
						splitedPairs.add(selectedPairList.get(start));
					}
					PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
					pluginConfig.put(HBaseReaderProperties.START_ROWKWY, splitedPairs.get(0).getFirst());
					pluginConfig.put(HBaseReaderProperties.END_ROWKWY, splitedPairs.get(splitedPairs.size() - 1).getSecond());
					list.add(pluginConfig);
				}
			} catch (IOException e) {
				LOGGER.error(Throwables.getStackTraceAsString(e));
			}

			return list;
		}
	}
}
