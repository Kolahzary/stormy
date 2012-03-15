package com.stormy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.Location;

public class Main {

	public static class TestBolt extends BaseRichBolt {
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "???"));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	public static class SimpleUDPServerSpout extends BaseRichSpout {
		DatagramSocket _server;
		SpoutOutputCollector _collector;
		byte[] _receiveData = new byte[1024];
		byte[] _sendData = new byte[1024];

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {

			if (this._server == null) {
				try {
					this._server = new DatagramSocket(9876);
				} catch (SocketException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			this._collector = collector;
		}

		@Override
		public void nextTuple() {
			DatagramPacket receivePacket = new DatagramPacket(
					this._receiveData, this._receiveData.length);
			try {
				this._server.receive(receivePacket);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String line = new String(receivePacket.getData());
			System.out.println("RECEIVED: " + line);
			this._collector.emit(new Values(line));

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		}

	}

	private static void setLatLon(String url, float lat, float lon) {
		URLConnection myURLConnection;
		try {
			URL myURL = new URL(url + "?lat=" + lat + "&long=" + lon);
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

	public static class IPParserBolt extends BaseRichBolt {
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			String[] data = tuple.getString(0).split(" ");
			for (int i = 0; i < data.length; i++) {
				if (data[i]
						.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
					_collector.emit(tuple, new Values(data[i]));
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("ip"));
		}
	}
	
	public static class GeoIPBolt extends BaseRichBolt {
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
