package com.radcheb.storm_test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PrimeNumberTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		final NumbersSpout numbersSpout = new NumbersSpout();
		builder.setSpout("spout", numbersSpout);
		builder.setBolt("prime", new PrimeNumberBolt())
				.shuffleGrouping("spout");
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(1000);
		System.out.println("The last number in spout: "
				+ numbersSpout.getLastNumber());
		cluster.killTopology("test");
		cluster.shutdown();
	}
}