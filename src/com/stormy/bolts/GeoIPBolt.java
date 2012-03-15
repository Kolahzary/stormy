package com.stormy.bolts;

import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class GeoIPBolt extends BaseRichBolt {
	OutputCollector _collector;
	LookupService _cl;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		String dbpath = "GeoIPCity.dat";
		try {
			_cl = new LookupService(dbpath, LookupService.GEOIP_STANDARD);
		} catch (IOException e) {
			System.out.println("IO Exception while initializing GeoIP database. It should be installed in " + dbpath);
		}
	}
	
	@Override
	public void execute(Tuple tuple) {
		String ip = tuple.getString(0);
		
		if (_cl != null) {
			Location loc;
			loc = _cl.getLocation(ip);
			if (loc != null) {
				_collector.emit(new Values(loc.longitude, loc.latitude));
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("long", "lat"));
	}
}

