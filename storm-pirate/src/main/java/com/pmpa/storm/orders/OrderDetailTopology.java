package com.pmpa.storm.orders;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class OrderDetailTopology {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("Spout", new OrdersSpout());
		builder.setBolt("Split", new OrderSplitBolt()).shuffleGrouping("Spout");
		builder.setBolt("OrderCount", new OrderCountBolt()).fieldsGrouping("Split", new Fields("Customer"));
		
		Config conf = new Config();
		conf.setMaxSpoutPending(10);
		
		//If not exists the arguments, submit the topology locally, or to the cluster
		if (args.length == 0 ){
			LocalCluster local = new LocalCluster();
			local.submitTopology("LocalWordCount", conf, builder.createTopology());
		}
		else{
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
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
