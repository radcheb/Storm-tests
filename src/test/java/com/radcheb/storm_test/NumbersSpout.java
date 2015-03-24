package com.radcheb.storm_test;

import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class NumbersSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private static int currentNumber = 1;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		// Emit the next number
		currentNumber = currentNumber + 2;
		collector.emit(new Values(new Integer(currentNumber)));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}

	public int getLastNumber() {
		return currentNumber;
	}
}