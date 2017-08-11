package com.pmpa.storm.wordcount;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * 
 * <p>Title: SentencesDataSource.java</p> 
 * <p>Description: 模拟不断向一个socket发送数据（一组sentences，随机发送其中某一行）</p>
 *
 * @author natty
 * @date 2017年8月2日
 * @version 1.0
 */

public class SentencesDataSource {
	
	private static final String[] sentences = {
			"At FullContact we currently use Storm as the backbone of the system which synchronizes our Cloud Address Book with third party services such as Google Contacts and Salesforce",
			"Spotify serves streaming music to over 10 million subscribers and 10 million active users",
			"hadoop hbase toyko hadoop hive",
			"hadoop spark spark hbase spark"
	};
	
	
	public static void main(String[] args) throws IOException {
		
		//服务器端生成ServerSocket对象，随时监听客户端。
		ServerSocket seserver =  new ServerSocket(9180);

		while(true){
			Socket sclient =  seserver.accept();

			OutputStream sclientoutput = sclient.getOutputStream();
			
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(sclientoutput));
			
			pw.write(sentences[new Random().nextInt(sentences.length)]);
			
			pw.flush();
			
			sclient.close();
		}

		
	}

}
