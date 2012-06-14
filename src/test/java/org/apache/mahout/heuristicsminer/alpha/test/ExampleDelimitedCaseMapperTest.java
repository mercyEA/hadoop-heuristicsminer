package org.apache.mahout.heuristicsminer.alpha.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.heuristicsminer.alpha.ExampleDelimitedCaseMapper;
import org.junit.Before;
import org.junit.Test;
import org.apache.mahout.heuristicsminer.message.CaseProto;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

/**
 * ExampleDelimitedCaseMapperTest tests the mapper ExampleDelimitedCaseMapper
 * 
 * @author John Leach
 * @version 1.0
 * 
 */
public class ExampleDelimitedCaseMapperTest {
	private Mapper<LongWritable, Text, ProtobufWritable<org.apache.mahout.heuristicsminer.message.CaseProto.Case>, ProtobufWritable<org.apache.mahout.heuristicsminer.message.CaseProto.Event>> mapper;
	private MapDriver<LongWritable, Text, ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>> mapDriver;
	
	@Before
	public void setUp() {
		mapper = new ExampleDelimitedCaseMapper();
		mapDriver = new MapDriver<LongWritable, Text, ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>> (mapper);
	}
	
	@Test
	public void testMap() throws IOException, ParseException {
		List<Pair<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>>> actual = new ArrayList<Pair<ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>>>();;
		JobConf conf = new JobConf();
		mapDriver.setConfiguration(conf);		
		actual = mapDriver.withInput(new LongWritable(0),new Text("case 1,activity A,John,09-03-2004 15:01:00")).run();
		Assert.assertEquals(1, actual.size());
		actual.get(0).getSecond().setConverter(CaseProto.Event.class);
		actual.get(0).getFirst().setConverter(CaseProto.Case.class);
		Assert.assertEquals("case 1",actual.get(0).getFirst().get().getCaseID());
		Assert.assertEquals(new SimpleDateFormat(ExampleDelimitedCaseMapper.dateTimeFormat).parse("09-03-2004 15:01:00").getTime(),actual.get(0).getFirst().get().getUnixTime());
		Assert.assertEquals("activity A",actual.get(0).getSecond().get().getEvent());
	}
	
	public List<ProtobufWritable<CaseProto.Event>> generateTestSet() throws IOException {
		List<ProtobufWritable<CaseProto.Event>> actual = new ArrayList<ProtobufWritable<CaseProto.Event>>();;
		JobConf conf = new JobConf();
		setUp();
		mapDriver.setConfiguration(conf);		
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity a,John,09-03-2004 15:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity b,John,09-03-2004 16:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity c,John,09-03-2004 17:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity d,John,09-03-2004 18:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity e,John,09-03-2004 19:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity f,John,09-03-2004 20:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity g,John,09-03-2004 21:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity h,John,09-03-2004 22:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity a,John,09-03-2004 22:05:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity i,John,09-03-2004 23:01:00")).run().get(0).getSecond());
		actual.add(mapDriver.withInput(new LongWritable(0),new Text("case 1,activity j,John,09-04-2004 01:01:00")).run().get(0).getSecond());
		return actual;
	}
	
	@Test 
	public void testMercyDateFormatParse() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		sdf.parse("2011-09-27 20:50:30.0");
	}
}
