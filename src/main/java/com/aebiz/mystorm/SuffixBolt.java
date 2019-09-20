package com.aebiz.mystorm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class SuffixBolt extends BaseBasicBolt{
	FileWriter fileWriter = null;
	
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			
			File file =new File("F:/hadoop/stormoutput/");
			if(!file.exists()){
				file.mkdirs();
			}
			fileWriter = new FileWriter("F:/hadoop/stormoutput/"+UUID.randomUUID());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//���õ���һ��������͹�������Ʒ����
				String upper_name = tuple.getString(0);
				
	
				String suffix_name = upper_name + "_*****************************";
				System.out.println(suffix_name);
				
				//Ϊ��һ��������͹�������Ʒ������Ӻ�׺
				
				try {
					fileWriter.write(suffix_name);
					fileWriter.write("\n");
					fileWriter.flush();
					
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
		
		
		
		
	}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
