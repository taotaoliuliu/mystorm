package com.aebiz.mystorm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class UpperBolt extends BaseBasicBolt{

	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		
		
		String myStr = tuple.getString(0);
		
		String upperCase = myStr.toUpperCase();
		
		collector.emit(new Values(upperCase));
		
	}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		
		declarer.declare(new Fields("uppername"));
		
	}

}
