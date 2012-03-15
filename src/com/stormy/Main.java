package com.stormy;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.stormy.bolts.GeoIPBolt;
import com.stormy.bolts.IPParserBolt;
import com.stormy.spouts.SimpleUDPServerSpout;

public class Main {

	private static void setLatLon(String url, float lat, float lon) {
		URLConnection myURLConnection;
		try {
			URL myURL = new URL(url + "?lat=" + lat + "&lon=" + lon);
			myURLConnection = myURL.openConnection();
			myURLConnection.connect();

			BufferedReader in = new BufferedReader(new InputStreamReader(
					myURLConnection.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null)
				System.out.println(inputLine);
			in.close();
		} catch (Exception e) {
			System.err.println("Error: " + e);
		}
	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line", new SimpleUDPServerSpout(), 1);
		builder.setBolt("ip", new IPParserBolt(), 3).shuffleGrouping("line");
		builder.setBolt("geo", new GeoIPBolt(), 3).shuffleGrouping("ip");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(1000000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
