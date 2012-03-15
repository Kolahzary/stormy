package com.stormy.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamSpout extends BaseRichSpout {
	
	String _username;
	String _password;
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> _queue = null;
	TwitterStream _twitterStream;
	
	public TwitterStreamSpout(String _u, String _p) {
		_username = _u;
		_password = _p;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_queue = new LinkedBlockingQueue<Status>(1000);
		
		StatusListener listener = new StatusListener() {
			
			@Override
			public void onStatus(Status status) {
				_queue.offer(status);
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {}
			
			@Override
			public void onTrackLimitationNotice(int i) {}
			
			@Override
			public void onScrubGeo(long latitude, long longitude) {}
			
			@Override
			public void onException(Exception e) {}
		};
		
		ConfigurationBuilder c = new ConfigurationBuilder();
		c.setUser(_username);
		c.setPassword(_password);
		
		_twitterStream = new TwitterStreamFactory(c.build()).getInstance();
		_twitterStream.addListener(listener);
		_twitterStream.sample();
	}
	
	@Override
	public void nextTuple() {
		Status tweet = _queue.poll();
		if (tweet == null)
			Utils.sleep(100);
		else
			_collector.emit(new Values(tweet));
	}
	
	@Override
	public void close() {
		_twitterStream.shutdown();
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}
	
	@Override
	public void ack(Object id) {}
	
	@Override
	public void fail(Object id) {}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}