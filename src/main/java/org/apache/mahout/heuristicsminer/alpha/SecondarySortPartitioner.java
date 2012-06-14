package org.apache.mahout.heuristicsminer.alpha;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.mahout.heuristicsminer.message.CaseProto;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class SecondarySortPartitioner extends Partitioner<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>> implements Configurable {
	Configuration conf;
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int getPartition(ProtobufWritable<CaseProto.Case> key,ProtobufWritable<CaseProto.Event> value, int numPartitions) {
		key.setConverter(CaseProto.Case.class);
		return (key.get().getCaseID().hashCode() & Integer.MAX_VALUE)  % numPartitions;
	}

}
