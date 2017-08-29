package com.pmpa.storm.words;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordsToplogy {

	public static void main(String[] args) {
		// Construct the topology, create the DAG for Words count.
		TopologyBuilder builder = new TopologyBuilder();
		//parallelism = 5   
		builder.setSpout("Spout", new SentenceSpout(),5);
		
		builder.setBolt("Split", new SplitBolt(), 5).shuffleGrouping("Spout");
		
		builder.setBolt("Count", new CountBolt(), 5).fieldsGrouping("Split", new Fields("Word"));
		
		Config conf = new Config();
		conf.setMaxSpoutPending(10);
		conf.setNumWorkers(10);
		
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
