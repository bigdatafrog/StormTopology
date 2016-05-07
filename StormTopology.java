package com.kafka.storm.integration;

import storm.kafka.KafkaSpout;
import java.io.FileWriter;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.LocalCluster;
import java.io.File;
import java.io.BufferedWriter;

public class StormTopology {
	private static final String KAFKA_TOPIC_SPOUT = "kafka-sentence-spout";
	private static final String REPORTER_BOLT = "reporter-bolt";
	
	public static void main(String args[]) throws Exception{
			KafkaSpout kspout = buildKafkaSentenceSpout();
			CounterBolt counterbolt = new CounterBolt();
			
			//Create Topology Builder object and wire the Spouts and Bolts
			TopologyBuilder tp = new TopologyBuilder();
			
			tp.setSpout(KAFKA_TOPIC_SPOUT, kspout, 1);
			tp.setBolt(REPORTER_BOLT, counterbolt).shuffleGrouping(KAFKA_TOPIC_SPOUT);
			
			Config cfg = new Config();
			//StormSubmitter.submitTopology("FLUME-KAFKA-STORM", cfg, tp.createTopology());
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("HelloStorm", cfg, tp.createTopology());
			
	}
	
	public static KafkaSpout buildKafkaSentenceSpout(){
		String zkHostPort = "localhost:2181";
	    String topic = "sink1";

	    String zkRoot = "/acking-kafka-sentence-spout";
	    String zkSpoutId = "acking-sentence-spout";
	    ZkHosts zkHosts = new ZkHosts(zkHostPort);
	    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
	    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
	    return kafkaSpout;
	}


}
