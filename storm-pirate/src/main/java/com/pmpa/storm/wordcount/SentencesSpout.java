package com.pmpa.storm.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentencesSpout implements IRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3923098633720728746L;
	private SpoutOutputCollector collector;

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// spout停止后关闭一些资源，就在该方法内执行
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		// 获取数据的逻辑，循环，向后边的bolt发射数据。
		Socket socket = null;
		
		try {
			socket = new Socket("StormCluster",9180);	
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String str = br.readLine();
			
			if(str != null){
				collector.emit(new Values(str));
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			try {
				socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}
	
	/**
	 * conf：该spout的storm配置
	 * context： 在topology中，task的位置。包括 task id，task的组件id，输入输出信息等。
	 * collector：用来从该spout发射元组。
	 */
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// 初始化操作
		this.collector = collector;
	}
	
	//声明key值。
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
