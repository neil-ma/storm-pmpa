package com.pmpa.storm.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * <p>Title: SplitBolt.java</p> 
 * <p>Description: 第一个处理bolt  </p>
 *
 * @author natty
 * @date 2017年8月4日
 * @version 1.0
 */

public class SplitBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3630470039036808852L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getStringByField("sentences");
		String[]words = sentence.split(" ");
		
		for(String word : words){
			collector.emit(new Values(word));
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("splitwords"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
