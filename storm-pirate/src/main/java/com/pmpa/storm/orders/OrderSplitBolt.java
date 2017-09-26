package com.pmpa.storm.orders;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderSplitBolt extends BaseRichBolt {
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector =  collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String orderdetail = input.getStringByField("OrderDetail");
		
		// array[0] CustomerNames  array[1] CommdityName array[2] price
		this.collector.emit(new Values(orderdetail.split("\t")[0],orderdetail.split("\t")[2]));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Customer","Price"));
	}

}
