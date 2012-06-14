package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.heuristicsminer.message.CaseProto;
import org.apache.mahout.heuristicsminer.message.CaseProto.BasicRelation;
import org.apache.mahout.heuristicsminer.message.CaseProto.RelationMetrics;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class RelationReducer extends Reducer<ProtobufWritable<CaseProto.Relation>,ProtobufWritable<CaseProto.RelationMetrics>,ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>>{
	ProtobufWritable<CaseProto.RelationMetrics> protoValue = ProtobufWritable.newInstance(CaseProto.RelationMetrics.class);
	public static final String DEPENDENCY_THRESHOLD = "DEPENDENCY_THRESHOLD";
	public static final String POSITIVE_OBSERVATIONSTHRESHOLD = "POSITIVE_OBSERVATIONS_THRESHOLD";
	public static final String RELATIVE_TO_BEST_THRESHOLD = "RELATIVE_TO_BEST_THRESHOLD";
	public static double dependencyThreshold = 0.45;
	public static Integer positiveObservationsThreshold = 1;
	public static double relativeToBestThreshold = 0.4;
	
	
	public static enum ErrorCounters {
		ERRORS
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dependencyThreshold = context.getConfiguration().get(DEPENDENCY_THRESHOLD) != null ? new Double(context.getConfiguration().get(DEPENDENCY_THRESHOLD)) : dependencyThreshold;
		positiveObservationsThreshold = context.getConfiguration().get(POSITIVE_OBSERVATIONSTHRESHOLD) != null ? new Integer(context.getConfiguration().get(POSITIVE_OBSERVATIONSTHRESHOLD)) : positiveObservationsThreshold;
		relativeToBestThreshold = context.getConfiguration().get(RELATIVE_TO_BEST_THRESHOLD) != null ? new Double(context.getConfiguration().get(RELATIVE_TO_BEST_THRESHOLD)) : relativeToBestThreshold;
	}

	@Override
	protected void reduce(ProtobufWritable<CaseProto.Relation> relation,Iterable<ProtobufWritable<CaseProto.RelationMetrics>> metrics,Context context) throws IOException, InterruptedException {
		try {
			CaseProto.RelationMetrics.Builder metricsBuilder = CaseProto.RelationMetrics.newBuilder();
			relation.setConverter(CaseProto.Relation.class);
			for (ProtobufWritable<RelationMetrics> metric: metrics) {
				metric.setConverter(CaseProto.RelationMetrics.class);
				metricsBuilder.setDirectlyPreceeds(metricsBuilder.getDirectlyPreceeds()+metric.get().getDirectlyPreceeds());
				metricsBuilder.setEventualyPreceeds(metricsBuilder.getEventualyPreceeds()+metric.get().getEventualyPreceeds());
				metricsBuilder.setShortLoopDirectlyPreceeds(metricsBuilder.getShortLoopDirectlyPreceeds()+metric.get().getShortLoopDirectlyPreceeds());
				metricsBuilder.setReverseDirectlyPreceeds(metricsBuilder.getReverseDirectlyPreceeds()+metric.get().getReverseDirectlyPreceeds());
				metricsBuilder.setReverseEventualyPreceeds(metricsBuilder.getReverseEventualyPreceeds()+metric.get().getReverseEventualyPreceeds());
				metricsBuilder.setReverseShortLoopDirectlyPreceeds(metricsBuilder.getReverseShortLoopDirectlyPreceeds()+metric.get().getReverseShortLoopDirectlyPreceeds());
				metricsBuilder.setCount(metricsBuilder.getCount()+metric.get().getCount());
			}			
			metricsBuilder.setDependencyRelation(computeDependencyRelation(metricsBuilder.getDirectlyPreceeds(),metricsBuilder.getReverseDirectlyPreceeds()));
			metricsBuilder.setLengthTwoLoopRelation(computeLengthTwoLoopRelation(metricsBuilder.getEventualyPreceeds(),metricsBuilder.getReverseEventualyPreceeds()));
			if (relation.get().getLeftSide() == relation.get().getRightSide()) {
				metricsBuilder.setLengthOneLoopRelation(metricsBuilder.getDirectlyPreceeds());
			}
			if (metricsBuilder.getDependencyRelation() > dependencyThreshold) {
				metricsBuilder.setBasicRelation(BasicRelation.DIRECT);
			} else if (metricsBuilder.getDirectlyPreceeds() > positiveObservationsThreshold && metricsBuilder.getReverseDirectlyPreceeds() > positiveObservationsThreshold) {
				metricsBuilder.setBasicRelation(BasicRelation.PARALLEL);
			} else {
				metricsBuilder.setBasicRelation(BasicRelation.NONE);
			}
			if (computeDependencyRelation(metricsBuilder.getReverseDirectlyPreceeds(),metricsBuilder.getDirectlyPreceeds()) > dependencyThreshold) {
				metricsBuilder.setIsInput(true);
			}
			protoValue.set(metricsBuilder.build());
			context.write(relation,protoValue);			
		} catch (Exception e) {
			e.printStackTrace();
			context.getCounter(ErrorCounters.ERRORS).increment(1);
		}
	}
	public static double computeDependencyRelation(Long leftRight, Long reverse) {
		return (leftRight.doubleValue()-reverse.doubleValue())/(leftRight.doubleValue()+reverse.doubleValue()+1); 
	}
	public static double computeLengthOneLoopRelation(Long leftRight) {
		return (leftRight.doubleValue())/(leftRight.doubleValue()+1);
	}
	public static double computeLengthTwoLoopRelation(Long leftRight,Long reverse) {
		return (leftRight.doubleValue()+reverse.doubleValue())/(leftRight.doubleValue()+reverse.doubleValue()+1); 
	}
	
}

