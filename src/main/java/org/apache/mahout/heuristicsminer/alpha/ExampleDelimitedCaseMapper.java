package org.apache.mahout.heuristicsminer.alpha;

import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.heuristicsminer.message.CaseProto;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class ExampleDelimitedCaseMapper extends Mapper<LongWritable, Text, ProtobufWritable<CaseProto.Case>, ProtobufWritable<CaseProto.Event>>{
	public static final String FILE_DELIMITER = "FILE_DELIMITER";
	public static final String CASE_POSITION = "CASE_POSITION";
	public static final String EVENT_POSITION = "EVENT_POSITION";
	public static final String DATE_TIME_POSITION = "DATE_TIME_POSITION";
	public static final String DATE_TIME_FORMAT = "DATE_TIME_FORMAT";
	
	public static String delimiter = ",";
	public static int eventPosition = 1;
	public static int casePosition = 0;
	public static int dateTimePosition = 3;
	public static String dateTimeFormat = "MM-dd-yyyy HH:mm:SS";
	
	public static SimpleDateFormat format;
	ProtobufWritable<CaseProto.Event> protoValue = ProtobufWritable.newInstance(CaseProto.Event.class);
	ProtobufWritable<CaseProto.Case> protoKey = ProtobufWritable.newInstance(CaseProto.Case.class);

	public static enum ErrorCounters {
		PARSING_ERRORS
	};
	
	public ExampleDelimitedCaseMapper() {
		
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		delimiter = context.getConfiguration().get(FILE_DELIMITER) != null ? context.getConfiguration().get(FILE_DELIMITER) : delimiter;
		eventPosition = context.getConfiguration().get(EVENT_POSITION) != null ? new Integer(context.getConfiguration().get(EVENT_POSITION)) : eventPosition;
		casePosition = context.getConfiguration().get(CASE_POSITION) != null ? new Integer(context.getConfiguration().get(CASE_POSITION)) : casePosition;
		dateTimePosition = context.getConfiguration().get(DATE_TIME_POSITION) != null ? new Integer(context.getConfiguration().get(DATE_TIME_POSITION)) : dateTimePosition;
		dateTimeFormat = context.getConfiguration().get(DATE_TIME_FORMAT) != null ? context.getConfiguration().get(DATE_TIME_FORMAT) : dateTimeFormat;
		format = new SimpleDateFormat(dateTimeFormat);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String[] row = value.toString().split(delimiter);
			CaseProto.Case.Builder caseBuilder = CaseProto.Case.newBuilder();
			caseBuilder.setCaseID(row[casePosition]);
			caseBuilder.setUnixTime(format.parse(row[dateTimePosition]).getTime());
			CaseProto.Event.Builder eventBuilder = CaseProto.Event.newBuilder();
			eventBuilder.setEvent(row[eventPosition]);
			protoKey.set(caseBuilder.build());
			protoValue.set(eventBuilder.build());			
			context.write(protoKey, protoValue);
		} catch (Exception e) {
			System.out.println("Parsing Errors");
			e.printStackTrace();
			context.getCounter(ErrorCounters.PARSING_ERRORS).increment(1);
		}
	}
}
