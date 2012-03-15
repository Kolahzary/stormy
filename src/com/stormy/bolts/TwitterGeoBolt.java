package com.stormy.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import twitter4j.GeoLocation;
import twitter4j.Status;

public class TwitterGeoBolt extends BaseRichBolt {
	
	OutputCollector _collector;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		Status s = (Status) tuple.getValue(0);
		GeoLocation g = s.getGeoLocation();
		if (g != null) {
			float longitude = (float) g.getLongitude();
			float latitude = (float) g.getLatitude();
			_collector.emit(new Values(longitude, latitude));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("long", "lat"));
	}
}