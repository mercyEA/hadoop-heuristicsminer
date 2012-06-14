package org.apache.mahout.heuristicsminer.app;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.heuristicsminer.alpha.CaseReducer;
import org.apache.mahout.heuristicsminer.alpha.CausalMatrixMapper;
import org.apache.mahout.heuristicsminer.alpha.CausalMatrixReducer;
import org.apache.mahout.heuristicsminer.alpha.ExampleDelimitedCaseMapper;
import org.apache.mahout.heuristicsminer.alpha.RelationReducer;
import org.apache.mahout.heuristicsminer.alpha.ScoredEventMapper;
import org.apache.mahout.heuristicsminer.alpha.ScoredEventReducer;
import org.apache.mahout.heuristicsminer.alpha.SecondarySortGroupComparator;
import org.apache.mahout.heuristicsminer.alpha.SecondarySortKeyComparator;
import org.apache.mahout.heuristicsminer.alpha.SecondarySortPartitioner;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
/**
 * 
 * Class that drives the process mining approach.
 * 
 * 
 * Usage:                                                                          
 * [-dependencyThreshold <threshold> -positiveObservationsThreshold <threshold>   
 * -relativeToBestThreshold <threshold> -delimiterInput <file> -delimiter <value>  
 * -output <file> -eventPosition <position> -casePosition <position>               
 * -dateTimePosition <position> -dateTimeFormat <format> -tempOutput <directory>]  
 * options                                                                         
 * -dependencyThreshold threshold              The dependency metric threshold   
 *                                             for relationship inclusion        
 *                                             (Between 1.0 and 0.1              
 * -positiveObservationsThreshold threshold    Need at least this number of      
 *                                             observations (1..N integers)      
 * -relativeToBestThreshold threshold          Location of join output files     
 * -delimiterInput file                        Location of delimiter input files 
 * -delimiter value                            The delimiter of the delimitted   
 *                                             input                             
 * -output file                                Location of the output result     
 * -eventPosition position                     The location of the event in the  
 *                                             delimitted set                    
 * -casePosition position                      The location of the case in the   
 *                                             delimitted set                    
 * -dateTimePosition position                  The location of the               
 *                                             dateTimeComponent in the          
 *                                             delimitted set                    
 * -dateTimeFormat format                      The dateTimeFormat for parsing    
 *                                             dateTime                          
 * -tempOutput directory                       Location of the temporary results 
 *
 * 
 * @author jleach
 *
 */
public class ProcessMining extends Configured implements Tool {
	public static CommandLine cmdLine;
	public static Option dependencyThreshold;	
	public static Option positiveObservationsThreshold;	
	public static Option relativeToBestThreshold;		
	public static Option delimiterInput;
	public static Option delimiter;
	public static Option eventPosition;
	public static Option casePosition;
	public static Option dateTimePosition;
	public static Option dateTimeFormat;
	public static Option output;
	public static Option tempOutput;
	
	
	public ProcessMining() {

	}

	/**  run - job command processor
	 * @param none
	 * @return 0 if success, -1 otherwise
	 * 
	 * */
	public int run(String[] args) throws Exception {

	    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    

		delimiter =
	    	    obuilder
	    	        .withShortName("delimiter")
	    	        .withDescription("The delimiter of the delimitted input")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("value")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault(",")
	    	                .create())
	    	        .create();	    	  
		eventPosition =
	    	    obuilder
	    	        .withShortName("eventPosition")
	    	        .withDescription("The location of the event in the delimitted set")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("position")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("1")
	    	                .create())
	    	        .create();	    	  
		casePosition =
	    	    obuilder
	    	        .withShortName("casePosition")
	    	        .withDescription("The location of the case in the delimitted set")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("position")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("0")
	    	                .create())
	    	        .create();	    	  
		dateTimePosition =
	    	    obuilder
	    	        .withShortName("dateTimePosition")
	    	        .withDescription("The location of the dateTimeComponent in the delimitted set")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("position")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("3")
	    	                .create())
	    	        .create();	    	  
		dateTimeFormat =
	    	    obuilder
	    	        .withShortName("dateTimeFormat")
	    	        .withDescription("The dateTimeFormat for parsing dateTime")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("format")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("MM-dd-yyyy HH:mm:SS")
	    	                .create())
	    	        .create();	    	  
			
	    dependencyThreshold =
	    	    obuilder
	    	        .withShortName("dependencyThreshold")
	    	        .withDescription("The dependency metric threshold for relationship inclusion (Between 1.0 and 0.1")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("threshold")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("0.45")
	    	                .create())
	    	        .create();	    	  
	    positiveObservationsThreshold =
	    	    obuilder
	    	        .withShortName("positiveObservationsThreshold")
	    	        .withDescription("Need at least this number of observations (1..N integers) ")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("threshold")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("1")
	    	                .create())
	    	        .create();	 

	    relativeToBestThreshold =
	    	    obuilder
	    	        .withShortName("relativeToBestThreshold")
	    	        .withDescription("Location of join output files")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("threshold")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .withDefault("0.4")
	    	                .create())
	    	        .create();	    
	    delimiterInput =
	    	    obuilder
	    	        .withShortName("delimiterInput")
	    	        .withRequired(true)
	    	        .withDescription("Location of delimiter input files")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("file")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .create())
	    	        .create();	     	    

	    output =
	    	    obuilder
	    	        .withShortName("output")
	    	        .withRequired(true)
	    	        .withDescription("Location of the output result")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("file")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .create())
	    	        .create();	     	    

	    tempOutput =
	    	    obuilder
	    	        .withShortName("tempOutput")
	    	        .withDescription("Location of the temporary results")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("directory")
	    	                .withDefault(System.currentTimeMillis() + "/")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .create())
	    	        .create();	     	    

	    Group options =
	    	    gbuilder
	    	        .withName("options")
	    	        .withOption(dependencyThreshold)
	    	        .withOption(positiveObservationsThreshold)
	    	        .withOption(relativeToBestThreshold)
	    	        .withOption(delimiterInput)
	    	        .withOption(delimiter)
	    	        .withOption(output)	    	        
	    	        .withOption(eventPosition)
	    	        .withOption(casePosition)
	    	        .withOption(dateTimePosition)
	    	        .withOption(dateTimeFormat)	
	    	        .withOption(tempOutput)
	 	            .create();	    
	    try {
	        Parser parser = new Parser();
	        parser.setGroup(options);
	        cmdLine = parser.parse(args);
	        if (cmdLine.hasOption("help")) {
	          CommandLineUtil.printHelp(options);
	          return -1;
	        }
	    } catch (OptionException e) {
	        CommandLineUtil.printHelp(options);
	        return -1;
	      }
	    runCaseRelationCreation();
	    runRelationComputations();
	    runEventClassification();
	    runCausalMatrix();
	    return 0;
	}

	/**
	 * 
	 * Runs the Case Relation Step that transforms event - case - timestamp to each event transition with metrics.
	 * 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws URISyntaxException
	 */
	public int runCaseRelationCreation() throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set(ExampleDelimitedCaseMapper.FILE_DELIMITER, (String) cmdLine.getValue(delimiter));
		conf.set(ExampleDelimitedCaseMapper.CASE_POSITION, (String) cmdLine.getValue(casePosition));
		conf.set(ExampleDelimitedCaseMapper.EVENT_POSITION, (String) cmdLine.getValue(eventPosition));
		conf.set(ExampleDelimitedCaseMapper.DATE_TIME_POSITION, (String) cmdLine.getValue(dateTimePosition));
		conf.set(ExampleDelimitedCaseMapper.DATE_TIME_FORMAT, (String) cmdLine.getValue(dateTimeFormat));
		Job job = new Job(conf);
		job.setJobName("Case Relation Creation on " + cmdLine.getValue(delimiterInput));
		job.setJarByClass(ProcessMining.class);
		TextInputFormat.addInputPath(job,new Path((String) cmdLine.getValue(delimiterInput)));
		job.setMapOutputKeyClass(ProtobufWritable.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setMapperClass(ExampleDelimitedCaseMapper.class);
		job.setReducerClass(CaseReducer.class);
		job.setOutputKeyClass(ProtobufWritable.class);
		job.setOutputValueClass(ProtobufWritable.class);
		job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setSortComparatorClass(SecondarySortKeyComparator.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path( ((String) cmdLine.getValue(tempOutput))+"caseRelation"));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);	
		job.setNumReduceTasks(32);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int runRelationComputations() throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();	
		conf.set(RelationReducer.DEPENDENCY_THRESHOLD,(String) cmdLine.getValue(dependencyThreshold));
		conf.set(RelationReducer.POSITIVE_OBSERVATIONSTHRESHOLD,(String) cmdLine.getValue(positiveObservationsThreshold));
		conf.set(RelationReducer.RELATIVE_TO_BEST_THRESHOLD,(String) cmdLine.getValue(relativeToBestThreshold));
		Job job = new Job(conf);
		job.setJobName("Run Relation Computations on " + cmdLine.getValue(tempOutput));
		job.setJarByClass(ProcessMining.class);
		SequenceFileInputFormat.addInputPath(job,new Path( ((String) cmdLine.getValue(tempOutput))+"caseRelation"));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(ProtobufWritable.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setReducerClass(RelationReducer.class);
		job.setOutputKeyClass(ProtobufWritable.class);
		job.setOutputValueClass(ProtobufWritable.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path( ((String) cmdLine.getValue(tempOutput))+"relationComputations"));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		job.setNumReduceTasks(32);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int runEventClassification() throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();	
		Job job = new Job(conf);
		job.setJobName("Run Event Classification on " + cmdLine.getValue(tempOutput) +"relationComputations");
		job.setJarByClass(ProcessMining.class);
		SequenceFileInputFormat.addInputPath(job,new Path( ((String) cmdLine.getValue(tempOutput))+"relationComputations"));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(ProtobufWritable.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setMapperClass(ScoredEventMapper.class);
		job.setReducerClass(ScoredEventReducer.class);
		job.setOutputKeyClass(ProtobufWritable.class);
		job.setOutputValueClass(ProtobufWritable.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path( ((String) cmdLine.getValue(tempOutput))+"eventClassification"));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public int runCausalMatrix() throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();	
		Job job = new Job(conf);
		job.setJobName("Run Causal Matrix on " + cmdLine.getValue(tempOutput) +"eventClassification");
		job.setJarByClass(ProcessMining.class);
		SequenceFileInputFormat.addInputPath(job,new Path( ((String) cmdLine.getValue(tempOutput))+"eventClassification"));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setMapperClass(CausalMatrixMapper.class);
		job.setReducerClass(CausalMatrixReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(job, new Path( ((String) cmdLine.getValue(output))));
		job.setOutputFormatClass(TextOutputFormat.class);		
		job.setNumReduceTasks(1);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ProcessMining(), args);
		System.exit(exitCode);
	}
  
}
