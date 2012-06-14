package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.hueristicsminer.message.CaseProto;
import org.apache.mahout.hueristicsminer.message.CaseProto.Case;
import org.apache.mahout.hueristicsminer.message.CaseProto.Event;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
/**
 * 
 * Reducer that takes in a case and its relations and outputs the relations and the metrics for the specific aggregated case.
 * 
 * @author jleach
 *
 */
public class CaseReducer extends Reducer<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>,ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>>{
	ProtobufWritable<CaseProto.Relation> protoKey = ProtobufWritable.newInstance(CaseProto.Relation.class);
	ProtobufWritable<CaseProto.RelationMetrics> protoValue = ProtobufWritable.newInstance(CaseProto.RelationMetrics.class);


	public static enum ErrorCounters {
		ERRORS
	}
	@Override
	protected void reduce(ProtobufWritable<Case> caseValue ,Iterable<ProtobufWritable<Event>> events,Context context) throws IOException, InterruptedException {
		try {
			Map<CaseProto.Event,Long> bagX = new LinkedHashMap<CaseProto.Event,Long>();	
			Map<CaseProto.Event, List<Integer>> bagY = new LinkedHashMap<CaseProto.Event, List<Integer>>();				
			int i = 0;
			List<Integer> sequence;
			for (ProtobufWritable<CaseProto.Event> protoEvent: events) {
				protoEvent.setConverter(CaseProto.Event.class);
				if (!bagY.containsKey(protoEvent.get()) && !bagX.containsKey(protoEvent.get())) {
					List<Integer> place = new ArrayList<Integer>();
					place.add(i);
					bagY.put(protoEvent.get(),place);
					bagX.put(protoEvent.get(),new Long(1));
				} else {
					sequence = bagY.get(protoEvent.get());
					sequence.add(i);
					bagY.put(protoEvent.get(), sequence);
					bagX.put(protoEvent.get(), bagX.get(protoEvent.get())+1);
				}
				i++;
			}			
			for (Event eventX : bagX.keySet()) {
				CaseProto.Relation.Builder relationBuilder = CaseProto.Relation.newBuilder();
				CaseProto.RelationMetrics.Builder relationMetricsBuilder = CaseProto.RelationMetrics.newBuilder();
				relationMetricsBuilder.setCount(bagX.get(eventX));
				relationBuilder.setLeftSide(eventX);
				protoKey.set(relationBuilder.build());
				protoValue.set(relationMetricsBuilder.build());
				context.write(protoKey, protoValue);
				for (Event eventY: bagY.keySet()) {
					relationBuilder = CaseProto.Relation.newBuilder();
					relationMetricsBuilder = CaseProto.RelationMetrics.newBuilder();
					relationBuilder.setLeftSide(eventX).setRightSide(eventY);
					relationMetricsBuilder.setDirectlyPreceeds(computeDirectlyPreceeds(bagY.get(eventX),bagY.get(eventY)));
					relationMetricsBuilder.setShortLoopDirectlyPreceeds(computeShortLoopDirectlyPreceeds(bagY.get(eventX),bagY.get(eventY)));
					relationMetricsBuilder.setEventualyPreceeds(computeEventuallyPreceeds(bagY.get(eventX),bagY.get(eventY)));
					relationMetricsBuilder.setReverseDirectlyPreceeds(computeDirectlyPreceeds(bagY.get(eventY),bagY.get(eventX)));
					relationMetricsBuilder.setReverseShortLoopDirectlyPreceeds(computeShortLoopDirectlyPreceeds(bagY.get(eventY),bagY.get(eventX)));
					relationMetricsBuilder.setReverseEventualyPreceeds(computeEventuallyPreceeds(bagY.get(eventY),bagY.get(eventX)));					
					protoKey.set(relationBuilder.build());
					protoValue.set(relationMetricsBuilder.build());
					context.write(protoKey, protoValue);	
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			context.getCounter(ErrorCounters.ERRORS).increment(1);
		}
	};
	
	
	public static int computeDirectlyPreceeds(List<Integer> leftList, List<Integer> rightList) {
		int i = 0;
		for (Integer left : leftList) {
			if (rightList.contains(left+1))
				i++;
		}
		return i;
	}

	public static int computeShortLoopDirectlyPreceeds(List<Integer> leftList, List<Integer> rightList) {
		int i = 0;
		for (Integer left : leftList) {
			if (rightList.contains(left+1) && leftList.contains(left+2))
				i++;
		}
		return i;
	}
	public static int computeEventuallyPreceeds(List<Integer> leftList, List<Integer> rightList) {
		int i = 0;
		Integer begin = null;
		Integer end = null;
		leftList.add(Integer.MAX_VALUE);
		for (Integer left : leftList) {	
			if (begin == null) {
				begin = left;
				continue;
			} else if (end == null) {
				end = left;
			} else {
				begin = end;
				end = left;
			}
			for (Integer right: rightList) {
				if (begin < right && end > right) {
					i++;
				}
			}
		}
		return i;
	}

}

