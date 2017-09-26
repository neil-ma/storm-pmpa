package com.pmpa.storm.words;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class CountBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3009642654496775732L;
	
	private Map<String,Long> output;
	private long count = 0L;
	private String sampleWord;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		output = new HashMap<String,Long>();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		sampleWord = input.getStringByField("Word");
		if (output.containsKey(sampleWord)){
			count = output.get(sampleWord) + 1;
			output.put(sampleWord, count);
		}
		else
		{
			count = 0L;
			output.put(sampleWord, count);
		}
		
		System.out.println("The word " + sampleWord + " => "+count);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
