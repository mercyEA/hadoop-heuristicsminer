package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.hueristicsminer.message.CaseProto;
import org.apache.mahout.hueristicsminer.message.CaseProto.BasicRelation;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class ScoredEventReducer extends Reducer<ProtobufWritable<CaseProto.Event>,ProtobufWritable<CaseProto.Evaluation>,ProtobufWritable<CaseProto.Event>,ProtobufWritable<CaseProto.ScoredEvent>>{
	ProtobufWritable<CaseProto.ScoredEvent> protoValue = ProtobufWritable.newInstance(CaseProto.ScoredEvent.class);

	public static enum ErrorCounters {
		ERRORS
	}

	@Override
	protected void reduce(ProtobufWritable<CaseProto.Event> key,Iterable<ProtobufWritable<CaseProto.Evaluation>> values,Context context) throws IOException, InterruptedException {
		CaseProto.ScoredEvent.Builder scoredEventBuilder = CaseProto.ScoredEvent.newBuilder();
		try {
			key.setConverter(CaseProto.Event.class);
			scoredEventBuilder.setEvent(key.get());
			scoredEventBuilder.setCount(0);
			for (ProtobufWritable<CaseProto.Evaluation> evaluation : values) {
				evaluation.setConverter(CaseProto.Evaluation.class);
				if (evaluation.get().getRelation().getRightSide() == null) {
					scoredEventBuilder.setCount(evaluation.get().getRelationMetrics().getCount());
				} else {
					scoredEventBuilder.addEvaluation(evaluation.get());
				}
				if (evaluation.get().getRelationMetrics().getBasicRelation().equals(BasicRelation.DIRECT)) {
					scoredEventBuilder.addOutputs(evaluation.get().getRelation().getRightSide());
				}
				if (evaluation.get().getRelationMetrics().getIsInput()) {
					scoredEventBuilder.addInputs(evaluation.get().getRelation().getRightSide());
				}
			}
			protoValue.set(scoredEventBuilder.build());
			context.write(key,protoValue);			
		} catch (Exception e) {
			e.printStackTrace();
			context.getCounter(ErrorCounters.ERRORS).increment(1);
		}
	}	
}