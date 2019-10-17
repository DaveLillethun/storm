/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter;

//added in                                                                          
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.net.SocketPermission;

import java.security.Permissions;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class AnchoredWordCount extends ConfigurableTopology {


    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new AnchoredWordCount(), args);
    }

    protected int run(String[] args) throws Exception {
       
	TopologyBuilder builder = new TopologyBuilder();
	Permissions PPPerm = new Permissions();
	PPPerm.add(new FilePermission("~/*", "read,write"));
        PPPerm.add(new SocketPermission("*", "connect"));

	Permissions EmmaPerm = new Permissions();
	EmmaPerm.add(new FilePermission("~/*", "read,write"));
        
	builder.setSpout("PPSpout", new PPSentenceSpout(), 4);
	builder.setPerm("PPSpout", PPPerm);
	//second spout
	builder.setSpout("EmmaSpout", new EmmaSentenceSpout(), 4);
	builder.setPerm("EmmaSpout", EmmaPerm);

	
        builder.setBolt("PPSplit", new SplitSentence(), 4).shuffleGrouping("PPSpout");
	builder.setBoltPerm("PPSplit", "PPSpout");
        builder.setBolt("EmmaSplit", new SplitSentence(), 4).shuffleGrouping("EmmaSpout");
	builder.setBoltPerm("EmmaSplit", "EmmaSpout");
	//third bolt
	builder.setBolt("WordCount", new WordCount(), 4).fieldsGrouping("PPSplit", new Fields("word")).fieldsGrouping("EmmaSplit", new Fields("word"));
	builder.setBoltPerm("WordCount", "PPSplit");
	builder.setBoltPerm("WordCount", "EmmaSplit");
	
        builder.setBolt("Output", new OutputBolt(), 4).fieldsGrouping("WordCount", new Fields("word"));
	builder.setBoltPerm("Output", "WordCount");
	
	builder.setBolt("MalOutput", new MalOutputBolt(), 4).fieldsGrouping("WordCount", new Fields("word"));
	builder.setBoltPerm("Output", "WordCount");
	
        Config conf = new Config();
        conf.setMaxTaskParallelism(3);

        String topologyName = "word-count";

        conf.setNumWorkers(3);

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }

    /*
    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector collector;
        Random random;


        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.random = new Random();
        }

        @Override
        public void nextTuple() {
            Utils.sleep(10);
            String[] sentences = new String[]{
                sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
                sentence("four score and seven years ago"),
                sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")
            };
            final String sentence = sentences[random.nextInt(sentences.length)];

            this.collector.emit(new Values(sentence), UUID.randomUUID());
        }

        protected String sentence(String input) {
            return input;
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
    */

    public static class SplitSentence extends BaseBasicBolt {
        
        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        
        }    

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                word = word.replaceAll("\\W", "");
		word = word.toLowerCase();
		collector.emit(new Values(word, 1));
            }
        }

        @Override
	public void cleanup() {
          
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            System.out.println("in wordCount: " + word);
	    Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }


    public static class PPSentenceSpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	Random random;
	Scanner scan;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    this.collector = collector;
	    this.random = new Random();
	    try{
                scan = new Scanner(new File("PPText.txt"));
            }
            catch(FileNotFoundException e){
                System.out.println("PP wouldn't open");
            }
	}
	
	@Override
	public void nextTuple() {
	    Utils.sleep(10);
	    String sentence = "";
	    if(scan.hasNextLine()){
		sentence = scan.nextLine();
		
	    }else{
		scan.close();
		System.out.println("PPSentenceSpout scan closed");
	    }
	    
	    this.collector.emit(new Values(sentence), UUID.randomUUID());
	}
	
	protected String sentence(String input) {
	    return input;
	}
	
	@Override
	public void ack(Object id) {
	}
	
	@Override
	public void fail(Object id) {
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word"));
	}
    }
    
    public static class EmmaSentenceSpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	Random random;
	Scanner scan;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    this.collector = collector;
	    this.random = new Random();
	    try{
                scan = new Scanner(new File("EmmaText.txt"));
            }catch(FileNotFoundException e){
                System.out.println("Emma wouldn't open");
            }
	}
	
	@Override
	public void nextTuple() {
	    Utils.sleep(10);
	    String sentence = "";
	    if(scan.hasNextLine()){
		sentence = scan.nextLine();
	    }else{
		scan.close();
	    }
	    
	    this.collector.emit(new Values(sentence), UUID.randomUUID());
	}
	
	protected String sentence(String input) {
	    return input;
	}
	
	@Override
	public void ack(Object id) {
	}
	
	@Override
	public void fail(Object id) {
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word"));
	}
    }
    
    
    //TODO: Create Output class where info gets written to file

    public static class OutputBolt extends BaseBasicBolt {

        @Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
	    int count = tuple.getInteger(0);
            System.out.println(word);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //declarer.declare(new Fields("word", "count"));
        }
    }
    
    //TODO: Create MalOutput class where illegal things are attempted

    public static class MalOutputBolt extends BaseBasicBolt {
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
        }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
    }
}
