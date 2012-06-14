package org.apache.mahout.heuristicsminer.cleanse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * A Class to reformat csv so it can be used in hive as a tab delimitted datasource.
 * 
 * @author jleach
 *
 */
public class MercyCSVCleanseMapper extends Mapper<LongWritable, Text, NullWritable, Text >{
	private Text textWritable = new Text();
	private static String QUOTE = "'";
	private static String DOUBLE_QUOTE = "\""; 
	private static String EMPTY = "";
	private static String COMMA = ",";
	private static String TAB = "\t";
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		textWritable.set(formatTSV(value.toString()));
		context.write(NullWritable.get(), textWritable);		
	}
	
	public static String formatTSV (String pseudoCSV) {
		StringBuffer sb = new StringBuffer();
		String[] commaSplits = pseudoCSV.split(COMMA);
		for (String split : commaSplits) {
			if ( (split.startsWith(QUOTE) || split.startsWith(DOUBLE_QUOTE)) && !split.endsWith(QUOTE) && !split.endsWith(DOUBLE_QUOTE) ) {
				sb.append(split.replaceFirst(QUOTE, EMPTY).replaceFirst(DOUBLE_QUOTE, EMPTY).trim());
				sb.append(COMMA);
			} else {
				sb.append(split.replaceAll(QUOTE, EMPTY).replaceAll(DOUBLE_QUOTE, EMPTY).replaceAll(TAB, EMPTY).trim());
				sb.append(TAB);
			}
		}
		if (sb.indexOf(TAB, sb.length() - 1) > 0) {
			sb.replace(sb.length() - 1, sb.length(), EMPTY);
		}
		if (sb.indexOf(COMMA, sb.length() - 1) > 0) {
			sb.replace(sb.length() - 1, sb.length(), EMPTY);
		}
		return sb.toString();
	}
	
}
