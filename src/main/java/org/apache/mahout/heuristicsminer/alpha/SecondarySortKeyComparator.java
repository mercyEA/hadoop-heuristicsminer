package org.apache.mahout.heuristicsminer.alpha;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.mahout.heuristicsminer.message.CaseProto;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class SecondarySortKeyComparator extends WritableComparator {

	protected SecondarySortKeyComparator() {
		super(ProtobufWritable.class,true);
	}
	
	protected SecondarySortKeyComparator(Class<? extends WritableComparable<ProtobufWritable<CaseProto.Case>>> keyClass) {
		super(keyClass);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ProtobufWritable<CaseProto.Case> aWritable = (ProtobufWritable<CaseProto.Case>) a;
		ProtobufWritable<CaseProto.Case> bWritable = (ProtobufWritable<CaseProto.Case>) b;
		aWritable.setConverter(CaseProto.Case.class);
		bWritable.setConverter(CaseProto.Case.class);
		int comp = aWritable.get().getCaseID().compareTo(bWritable.get().getCaseID());
		if (comp == 0)
			comp = new Long(aWritable.get().getUnixTime()).compareTo(new Long(bWritable.get().getUnixTime()));
		return comp;
	}
}
