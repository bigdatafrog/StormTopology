package com.storm.topology;

import com.storm.topology.WordCounterBolt;
import com.storm.topology.WordSplitterBolt;
import com.storm.topology.LineReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class HelloStorm {

	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("word-spitter", new WordSplitterBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spitter");
		
		//LocalCluster cluster = new LocalCluster();
		StormSubmitter.submitTopology("HelloStorm", config, builder.createTopology());
		//cluster.submitTopology("HelloStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		//cluster.shutdown();
	}

}
