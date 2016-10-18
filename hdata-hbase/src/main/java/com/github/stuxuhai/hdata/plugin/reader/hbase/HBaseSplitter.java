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
				 * return : Pair<[startKey1, startKey2, startKey3],
				 * 				 [endKey1  , endKey2  , endKey3  ]>
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

				/**
				 * startKeys.length == 1 说明只有一个 region
				 */
				if (startKeys.length == 1) {
					Pair<byte[], byte[]> pair = new Pair<>();
					pair.setFirst(startRowkeyBytes);
					pair.setSecond(endRowkeyBytes);
					selectedPairList.add(pair);
				} else {
					/**
					 * startRowkeyBytes.length 和 endRowkeyBytes.length 同时等于 0， 说明应该抽取所有数据
					 */
					if (startRowkeyBytes.length == 0 && endRowkeyBytes.length == 0) {
						/**
						 * 分别把每个 region 的 startKey 及 endKey 作为分解任务的界限
						 * 任务数等于 region 的总数
						 */
						for (int i = 0, len = startKeys.length; i < len; i++) {
							Pair<byte[], byte[]> pair = new Pair<>();
							pair.setFirst(startKeys[i]);
							pair.setSecond(endKeys[i]);
							selectedPairList.add(pair);
						}
					}
					/**
					 * startRowkeyBytes.length != 0， 而 endRowkeyBytes.length == 0,说明应该从 startRowkey 开始抽数，一直到抽完所有数据
					 */
					else if (endRowkeyBytes.length == 0) {
						for (int i = 0, len = startKeys.length; i < len; i++) {
							/**
							 * Bytes.compareTo 方法返回值解释： 0 if equal, < 0 if left is less than right, etc.
                             * 如果当前 region 的 endKeys 大于抽数任务的 startRowkey，说明该 region 有部分（也可能是整个 region）数据需要被抽取
							 */
							if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0) {
								Pair<byte[], byte[]> pair = new Pair<>();
								/**
                                 * 如果当前 startKey 大于抽数任务的 startRowkey，说明该 region 应该从 startKey 开始抽数，一直到 endKey，
								 * 否则该 region 应该从 startRowkey 开始抽数，一直到 endKey
								 */
								pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i] : startRowkeyBytes);
								pair.setSecond(endKeys[i]);
								selectedPairList.add(pair);
							}
						}
					}
					/**
					 * startRowkeyBytes.length 和 endRowkeyBytes.length 都不等于 0， 说明应该从 startRowkey 开始抽数，一直到 endRowkey
					 */
					else {
						for (int i = 0, len = startKeys.length; i < len; i++) {
							/**
                             * len == 1，说明只有一个 region，因此直接设置 startRowkey 和 endRowkey 即可
							 */
							if (len == 1) {
								Pair<byte[], byte[]> pair = new Pair<>();
								pair.setFirst(startRowkeyBytes);
								pair.setSecond(endRowkeyBytes);
								selectedPairList.add(pair);
								break;
							}
							/**
							 * 1、如果当前 region 的 endKey >= startRowkey，说明可能该 region 的部分（也可能是整个 region）数据需要被抽取，否则没有数据，直接进入下一循环
							 * 2、同时，当前 region 的 startKey 满足 endRowkey >= startKey，说明该 region 的数据范围没有超出任务的抽数范围，至少部分数据需要被抽取
							 *
							 *	任务配置必然条件： 	startRowkey >= endRowkey
							 *	同时，以下三条件成立：	endKey 		>= startRowkey
							 *						endRowkey 	>= startKey
							 *	因此推出结果: start <= startRowkey <= endRowkey <= endKey
							 */
							else if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0 && Bytes.compareTo(endRowkeyBytes, startKeys[i]) >= 0) {
								Pair<byte[], byte[]> pair = new Pair<>();
								pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i] : startRowkeyBytes);
								pair.setSecond(Bytes.compareTo(endKeys[i], endRowkeyBytes) <= 0 ? endKeys[i] : endRowkeyBytes);
								selectedPairList.add(pair);
							}
						}
					}
				}

				/**
				 * 如果并发度 parallelism 大于应该被抽取的 region 个数，则把 parallelism 设置为 region 的个数
				 */
				if (parallelism > selectedPairList.size()) {
					LOGGER.info(
							"parallelism: {} is greater than the region count: {} in the currently open table: {}, so parallelism is set equal to region count.",
							parallelism, selectedPairList.size(), table.getName().getNameAsString());
					parallelism = selectedPairList.size();
				}

				/**
				 * HBase 抽数并行度的切分的关键
				 */
				double step = (double) selectedPairList.size() / parallelism;
				for (int i = 0; i < parallelism; i++) {
					List<Pair<byte[], byte[]>> splitedPairs = new ArrayList<>();

					/**
					 * 为 region 按顺序分组
					 * 从 region 读取出来的 startKey 和 endKey 是有序的吗？
					 * 如果不是有序的，那这个算法可以用吗？
					 */
					for (int start = (int) Math.ceil(step * i), end = (int) Math .ceil(step * (i + 1)); start < end; start++) {
						splitedPairs.add(selectedPairList.get(start));
					}

					/**
					 * 克隆 readerConfig，并设置新的 startRowkey 和 endRowkey
					 */
					PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();

					/**
					 * 取该组 region 的第一 region 的第一列，作为该子任务配置的抽数起始位置
					 */
					pluginConfig.put(HBaseReaderProperties.START_ROWKWY, splitedPairs.get(0).getFirst());

					/**
					 * 取该组 region 的最后 region 的第二列，作为该子任务配置的抽数结束位置
					 */
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
