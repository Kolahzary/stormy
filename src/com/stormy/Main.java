package com.stormy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.stormy.bolts.GeoIPBolt;
import com.stormy.bolts.IPParserBolt;
import com.stormy.bolts.LongLatPusherBolt;
import com.stormy.spouts.SimpleUDPServerSpout;

public class Main {

    public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line", new SimpleUDPServerSpout(), 1);
		builder.setBolt("ip", new IPParserBolt(), 3).shuffleGrouping("line");
		builder.setBolt("geo", new GeoIPBolt(), 3).shuffleGrouping("ip");
		builder.setBolt("pusher", new LongLatPusherBolt(), 1).shuffleGrouping("geo");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(1000000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
