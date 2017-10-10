package com.pmpa.storm.wordstrident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;

/**
 * 
 * <p>Title: WordCountTridentTopology.java</p> 
 * <p>Description: Trident topology的简单例子实现</p>
 *
 * @author natty
 * @date 2017年10月9日
 * @version 1.0
 */

public class WordCountTridentTopology {

	public static void main(String[] args) {
		
		//原来使用的是TopologyBuilder，为了创建Trident，需要使用TridentTopology API
		TridentTopology topology = new TridentTopology();
		
		//之前继承IRichSpout是逐条处理的，现在需要一个封装多条tuple为一个batch的spout来替代原有的spout。这里使用测试的FixedBatchSpout
		/*
		 * public FixedBatchSpout(Fields fields,
                       int maxBatchSize,
                       List<Object>... outputs)
		 */
		//把所有记录分为2个批次发送，但是每次都是发布全部这些记录。
		@SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"),2,
				new Values("amzon alibaba google baidu tencent"),
				new Values("ford toyota dasauto bmw benz"),
				new Values("hadoop spark mapreduce flink beam"),
				new Values("USA UK Japan Germany France")
				);
		//如果不设置，spout在发送完一个batch之后就会结束发射。
		spout.setCycle(true);
		
		//体现为流式处理，返回类型为Stream
		topology.newStream("wordcount", spout)
		//输入field，中间处理函数（类似splitBolt的功能），发射到后边的field  sentenc --> words 
		.each(new Fields("sentence"), new SplitFunction(), new Fields("word"))
		//并行度控制，由2个task来执行这个bolt（这里是function，类似bolt的功能）
		.parallelismHint(2)
		//等同于fieldsGrouping
		.groupBy(new Fields("word"))
		//persistentAggregate会帮助你把一个状态源聚合的结果存储或者更新到存储当中。下面的示例直接存到内存中。
		//persistentAggregate方法会把数据流转换成一个TridentState对象。   Count是Trident内置的聚合函数。
		.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
		//为了显示最终的结果，这里使用一个filter来将内存中记录的每个单词的统计结果打印出来。因为上一步已经转化为了TridentState对象，现在需要再次转化为Stream。
		//PrintOutputFilter是一个实现了filter接口的类，在isKeep()方法中，如果返回true，则保留这条记录，否则干掉
		.newValuesStream()
		.each(new Fields("word","count"), new PrintOutputFilter("word","count"))
		;
		
		
		Config conf = new Config();
		
		if(args.length == 0 )
		{
			//开始提交topology，如果无参数是本地测试：
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcountTrident", conf, topology.build());
		}
		else
		{
			try {
				StormSubmitter.submitTopology("wordcountTridentCluster", conf, topology.build());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		


	}

}
