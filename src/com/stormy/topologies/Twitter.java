package com.stormy.topologies;

import backtype.storm.topology.TopologyBuilder;

import com.stormy.bolts.LongLatPusherBolt;
import com.stormy.bolts.TwitterGeoBolt;
import com.stormy.spouts.TwitterStreamSpout;

public class Twitter {
	
	private static final String USERNAME = "";
	private static final String PASSWORD = "";
	
	public static TopologyBuilder factory() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stream", new TwitterStreamSpout(USERNAME, PASSWORD), 1);
		builder.setBolt("geo", new TwitterGeoBolt(), 3).shuffleGrouping("stream");
		builder.setBolt("pusher", new LongLatPusherBolt(), 3).shuffleGrouping("geo");
		return builder;
	}
}