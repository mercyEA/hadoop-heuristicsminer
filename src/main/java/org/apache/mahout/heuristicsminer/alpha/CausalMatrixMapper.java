package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.heuristicsminer.message.CaseProto;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class CausalMatrixMapper extends Mapper<ProtobufWritable<CaseProto.Event>, ProtobufWritable<CaseProto.ScoredEvent>, Text, ProtobufWritable<CaseProto.ScoredEvent>>{	
	Text text = new Text("1");
	public CausalMatrixMapper() {
		
	}
	
	@Override
	protected void map(ProtobufWritable<CaseProto.Event> key, ProtobufWritable<CaseProto.ScoredEvent> value, Context context) throws IOException, InterruptedException {
		value.setConverter(CaseProto.ScoredEvent.class);
		context.write(text, value);
	}
}
