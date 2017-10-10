package com.pmpa.storm.wordstrident;

import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * <p>Title: PrintOutputFilter.java</p> 
 * <p>Description: 为了方便检查 wordcount Trident的执行情况，使用filter来打印最终的统计结果</p>
 *
 * @author natty
 * @date 2017年10月9日
 * @version 1.0
 */
public class PrintOutputFilter implements Filter {

	private static final long serialVersionUID = -5018234693968741550L;
	private String[] fields;
	
	
	//构造方法初始化每个field的值（读取key的名称）。
	public PrintOutputFilter(String... fields)
	{
		this.fields = fields;
	}
	

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		
		
		StringBuilder sb = new StringBuilder();
		
		for(String field:this.fields){
			sb.append(tuple.getValueByField(field));
			sb.append("\t");
		}
		
		System.err.println(sb.toString());
		return true;
	}

}
