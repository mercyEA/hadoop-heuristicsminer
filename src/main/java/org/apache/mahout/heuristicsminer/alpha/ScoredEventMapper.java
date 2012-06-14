package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.heuristicsminer.message.CaseProto;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class ScoredEventMapper extends Mapper<ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>, ProtobufWritable<CaseProto.Event>, ProtobufWritable<CaseProto.Evaluation>>{	
	ProtobufWritable<CaseProto.Evaluation> protoValue = ProtobufWritable.newInstance(CaseProto.Evaluation.class);
	ProtobufWritable<CaseProto.Event> protoKey = ProtobufWritable.newInstance(CaseProto.Event.class);
	public ScoredEventMapper() {
		
	}
	
	@Override
	protected void map(ProtobufWritable<CaseProto.Relation> key, ProtobufWritable<CaseProto.RelationMetrics> value, Context context) throws IOException, InterruptedException {
		CaseProto.Evaluation.Builder evaluationBuilder = CaseProto.Evaluation.newBuilder();
		key.setConverter(CaseProto.Relation.class);
		value.setConverter(CaseProto.RelationMetrics.class);
		evaluationBuilder.setRelation(key.get());
		evaluationBuilder.setRelationMetrics(value.get());
		protoValue.set(evaluationBuilder.build());
		protoKey.set(key.get().getLeftSide());
		context.write(protoKey, protoValue);
	}
}
