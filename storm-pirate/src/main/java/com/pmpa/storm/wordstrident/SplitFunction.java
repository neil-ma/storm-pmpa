package com.pmpa.storm.wordstrident;


import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * <p>Title: SplitFunction.java</p> 
 * <p>Description: 单词拆分的bolt，输入为sentence，输出为words，空格分隔    功能等同于前边开发的SplitBolt </p>
 * <p>相比于SplitBolt，这里不需要再declarer.declare过程，因为在tridenttopology中的each方法中已经指定了输出的fields </p>
 *
 * @author natty
 * @date 2017年10月9日
 * @version 1.0
 */

public class SplitFunction implements Function {

	private static final long serialVersionUID = 1710776221954767548L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words)
		{
			collector.emit(new Values(word));
		}
	}

}
