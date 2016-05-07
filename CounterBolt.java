package com.kafka.storm.integration;

import backtype.storm.task.OutputCollector;
import java.io.IOException;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class CounterBolt extends BaseRichBolt{
	private static final long serialVersionUID = 3092938699134129356L;
	  
	  private OutputCollector collector;
	  @Override @SuppressWarnings("rawtypes")
	  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
	    collector = outCollector;
	  }
	  
	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word"));
	  }
	  @Override
	  public void execute(Tuple tuple) {
	    Object value = tuple.getValue(0);
	    String sentence = null;
	    if (value instanceof String) {
	      sentence = (String) value;

	    } else {
	      // Kafka returns bytes
	      byte[] bytes = (byte[]) value;
	      try {
	        sentence = new String(bytes, "UTF-8");
	      } catch (UnsupportedEncodingException e) {
	        throw new RuntimeException(e);
	      }      
	    }

	    String[] words = sentence.split("\\s+");
	    for (String word : words) {
	    	System.out.println("word" + word);
/*Write to a flat file*/
			try{
			File file = new File("/home/sekar/filename.txt");
			// if file doesnt exists, then create it
						if (!file.exists()) {
							file.createNewFile();
						}
						FileWriter fw = new FileWriter(file.getAbsoluteFile());
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(word);
						bw.close();			
			}catch (IOException e){
				e.printStackTrace(); 
			}
			/*Write to a flat file*/
	    }
	    }
}
