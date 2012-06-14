package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.hueristicsminer.message.CaseProto;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class CausalMatrixReducer extends Reducer<Text,ProtobufWritable<CaseProto.ScoredEvent>,NullWritable,Text>{
	Text text = new Text();

	public static enum ErrorCounters {
		ERRORS
	}

	@Override
	protected void reduce(Text key,Iterable<ProtobufWritable<CaseProto.ScoredEvent>> values,Context context) throws IOException, InterruptedException {
		try {
			for (ProtobufWritable<CaseProto.ScoredEvent> scoredEvent : values) {
				StringBuffer sb = new StringBuffer();
				scoredEvent.setConverter(CaseProto.ScoredEvent.class);
				sb.append(scoredEvent.get().getEvent().getEvent());
				sb.append(",");
				sb.append("Input (");
				for (CaseProto.Event input : scoredEvent.get().getInputsList()) {
					sb.append(input.getEvent() + ",");
				}
				if (sb.indexOf(",", sb.length() - 1) > 0) {
					sb.replace(sb.length() - 1, sb.length(), "");
				}
				sb.append("), Ouput (");
				for (CaseProto.Event output : scoredEvent.get().getOutputsList()) {
					sb.append(output.getEvent());
					sb.append(",");
				}
				if (sb.indexOf(",", sb.length() - 1) > 0) {
					sb.replace(sb.length() - 1, sb.length(), "");
				}				
				sb.append(")");
				text.set(sb.toString());
				context.write(NullWritable.get(), text);
			}
		} catch (Exception e) {
			e.printStackTrace();
			context.getCounter(ErrorCounters.ERRORS).increment(1);
		}
	}	
}