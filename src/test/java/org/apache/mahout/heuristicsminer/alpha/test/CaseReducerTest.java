package org.apache.mahout.heuristicsminer.alpha.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.heuristicsminer.alpha.CaseReducer;
import org.junit.Before;
import org.junit.Test;
import org.apache.mahout.heuristicsminer.message.CaseProto;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class CaseReducerTest {
	private Reducer<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>,ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>> reducer;
	private ReduceDriver<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>,ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>> reduceDriver;
	ExampleDelimitedCaseMapperTest exampleDelimitedCaseMapperTest;	
		
	@Before
	public void setUp() {
		reducer = new CaseReducer();
		reduceDriver = new ReduceDriver<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>,ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>>(reducer);
		exampleDelimitedCaseMapperTest = new ExampleDelimitedCaseMapperTest();
	}
	
	/**  testReduce tests reducer generating feature-info protoBuff Messages
	 * @param none
	 * @return none
	 * 
	 * */
	@Test
	public void testReduce() throws IOException {
		List<Pair<ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>>> actual = new ArrayList<Pair<ProtobufWritable<CaseProto.Relation>, ProtobufWritable<CaseProto.RelationMetrics>>>();;
		ProtobufWritable<CaseProto.Case> protoKey = ProtobufWritable.newInstance(CaseProto.Case.class);
		CaseProto.Case.Builder builder = CaseProto.Case.newBuilder();
		builder.setCaseID("case 1");
		protoKey.set(builder.build());
		actual = reduceDriver.withInput(protoKey, exampleDelimitedCaseMapperTest.generateTestSet()).run();
		Assert.assertEquals(110,actual.size());
	}	
	
	@Test
	public void testComputeDirectlyPreceeds() {
		List<Integer> left = new ArrayList<Integer>();
		List<Integer> right = new ArrayList<Integer>();
		left.add(1);
		left.add(3);
		left.add(5);
		left.add(9);
		right.add(2);
		right.add(4);		
		Assert.assertEquals(2,CaseReducer.computeDirectlyPreceeds(left, right));
	}
	
	@Test
	public void testComputeShortLoopDirectlyPreceeds() {
		List<Integer> left = new ArrayList<Integer>();
		List<Integer> right = new ArrayList<Integer>();
		left.add(1);
		left.add(3);
		left.add(5);
		left.add(9);
		right.add(2);
		right.add(4);		
		Assert.assertEquals(2,CaseReducer.computeShortLoopDirectlyPreceeds(left, right));	
	}
	
	@Test
	public void testComputeEventuallyPreceeds() {
		List<Integer> left = new ArrayList<Integer>();
		List<Integer> right = new ArrayList<Integer>();
		left.add(1);
		left.add(3);
		left.add(5);
		left.add(9);
		right.add(2);
		right.add(4);		
		Assert.assertEquals(2,CaseReducer.computeEventuallyPreceeds(left, right));	
	}
}