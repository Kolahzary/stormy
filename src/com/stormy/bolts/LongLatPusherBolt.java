package com.stormy.bolts;
import java.util.Map;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LongLatPusherBolt extends BaseRichBolt {
	OutputCollector _collector;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		float lon = tuple.getFloat(0);
		float lat = tuple.getFloat(1);
		setLatLon("http://localhost:8080/", lat, lon);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}
	
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
}