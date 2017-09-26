package com.pmpa.storm.orders;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OrdersSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	
	private String[] names = {"John","Green","Lily","Jucie","Monica","James"};
	private String[] commoditiesNames = {"miPhone","MacPro","Honda","Porsche","PlaneBoring"};
	private String randomcommodity;
	private Map<String,Long> commodities;
	
	
	
	@Override
	//初始化操作
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.commodities = new HashMap<String,Long>();
		this.commodities.put("miPhone", 1000L);
		this.commodities.put("MacPro", 10000L);
		this.commodities.put("Honda", 100000L);
		this.commodities.put("Porsche", 1000000L);
		this.commodities.put("PlaneBoring", 10000000L);
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
		this.randomcommodity = 
				this.commoditiesNames[new Random().nextInt(this.commoditiesNames.length)];
		
		StringBuffer sb = new StringBuffer();
		sb.append(this.names[new Random().nextInt(this.names.length)]);
		sb.append("\t");
		sb.append(this.randomcommodity);
		sb.append("\t");
		sb.append(this.commodities.get(this.randomcommodity));
		
		
		System.out.println("=============================Order Details============================== : " + sb.toString());
		
		this.collector.emit(new Values(sb.toString()));
		
		try {
			Thread.sleep(3000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("OrderDetail"));
	}

}
