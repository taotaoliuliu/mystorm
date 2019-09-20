package com.aebiz.mystorm;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class MySpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	String[] num={"taotao","yuyu","liuliu","lili","shengsheng","love","you"};
	
	public void nextTuple() {
		Random random = new Random();

		int nextInt = random.nextInt(num.length);

		String randomStr = num[nextInt];

		collector.emit(new Values(randomStr));

		Utils.sleep(1000);

	}
	
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector=collector;
	}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orignname"));		
	}

}
