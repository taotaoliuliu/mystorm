package com.aebiz.mystorm;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class ToPoMain {

	public static String NIMBUS_HOST="127.0.0.1";
	public static void main(String[] args) {
		
		TopologyBuilder builder =new TopologyBuilder();
		
		builder.setSpout("myspout", new MySpout(), 4).setNumTasks(8);
		
		
		builder.setBolt("upper", new UpperBolt(), 4).shuffleGrouping("myspout");
		
		
		builder.setBolt("suffix", new SuffixBolt(), 4).shuffleGrouping("upper");
		
		StormTopology demotop = builder.createTopology();
				
				
				Config conf =new Config();
				
				conf.setNumWorkers(4);
				conf.setDebug(true);
				conf.setNumAckers(0);
				
				conf.put(Config.NIMBUS_HOST,NIMBUS_HOST); //配置nimbus连接主机地址，比如：192.168.10.1  
				conf.put(Config.NIMBUS_THRIFT_PORT,6627);    //int is expected here
				
				//conf.set
				
				
				//已这种方式提交必须配置zookeper
				conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("127.0.0.1")); // 配置zookeeper连接主机地址，可以使用集合存放多个
			    conf.put(Config.STORM_ZOOKEEPER_PORT, 2181); // 配置zookeeper连接端口，默认2181
				
				System.setProperty("storm.jar", "F:/aa.jar");   //link to exact file location (w/ dependencies)
				
				try {
					StormSubmitter.submitTopology("myaa", conf, demotop);
					
					/*LocalCluster cluster = new LocalCluster();
					cluster.submitTopology("HotProductTopology", conf, builder.createTopology());  
					Utils.sleep(30000); 
					cluster.shutdown();*/
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

		
	}
}
