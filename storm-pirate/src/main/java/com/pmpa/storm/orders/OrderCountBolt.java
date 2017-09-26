package com.pmpa.storm.orders;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class OrderCountBolt extends BaseRichBolt{
	
	private Map<String, Long> output;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		output = new HashMap<String,Long>();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String customer = input.getStringByField("Customer");
		String price = input.getStringByField("Price");
		String orderstatus ; 
		
		if(output.containsKey(customer)){
			output.put(customer, output.get(customer) + Long.parseLong(price));
		}
		else
		{
			output.put(customer, Long.parseLong(price));
		}
		
		System.out.print("++++++++++++++++++++++Current Status:");

		for(Iterator it = output.entrySet().iterator(); it.hasNext();){
			System.out.print(it.next() + " | ");
		}
		System.out.println("");
		
		
		for(String customername : output.keySet() )
		{
			if( output.get(customername) >=100000000L )
			{
				System.out.println("Congratulations!!!!  "+customername + "购买额达到100000000元!!!!");
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
