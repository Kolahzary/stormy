package com.stormy.spouts;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SimpleUDPServerSpout extends BaseRichSpout {
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
		DatagramPacket receivePacket = new DatagramPacket(this._receiveData,
				this._receiveData.length);
		try {
			this._server.receive(receivePacket);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String line = new String(receivePacket.getData());
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
