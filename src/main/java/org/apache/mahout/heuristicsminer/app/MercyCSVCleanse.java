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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.heuristicsminer.cleanse.MercyCSVCleanseMapper;

/**
 * 
 * Class for cleansing CSV files without proper encoding
 * 
 * @author jleach
 *
 */
public class MercyCSVCleanse extends Configured implements Tool {
	public static CommandLine cmdLine;
	public static Option csvInputOption;	
	public static Option csvOutputOption;	
	public static Option clean;
	public static Option synchronous;
	
	public MercyCSVCleanse() {

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
	    csvInputOption =
	    	    obuilder
	    	        .withShortName("csvInput")
	    	        .withRequired(true)
	    	        .withDescription("Location of the csv input files base directory (base_dir/table/files)")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("file")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .create())
	    	        .create();	    	  
	    csvOutputOption =
	    	    obuilder
	    	        .withShortName("csvOutput")
	    	        .withRequired(true)
	    	        .withDescription("Location of the snappy compressed csv sequence output files(output_dir/table/files")
	    	        .withArgument(
	    	            abuilder
	    	                .withName("file")
	    	                .withMinimum(1)
	    	                .withMaximum(1)
	    	                .create())
	    	        .create();	 
	    clean =
	    	    obuilder
	    	        .withShortName("clean")
	    	        .withDescription("Clean the Output Directory")
	    	        .create();	    
	    synchronous =
	    	    obuilder
	    	        .withShortName("synchronous")
	    	        .withDescription("Run Synchronously, defaults to submitting all jobs at once")
	    	        .create();	
	    Group options =
	    	    gbuilder
	    	        .withName("options")
	    	        .withOption(csvInputOption)
	    	        .withOption(csvOutputOption)	    	        
	    	        .withOption(clean)	    	        
	    	        .withOption(synchronous)	    	        
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
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);

	    if (cmdLine.hasOption(clean)) {
	    	fs.delete(new Path((String) cmdLine.getValue(csvOutputOption)),true);
	    }
	    FileStatus[] file_status = fs.listStatus(new Path((String) cmdLine.getValue(csvInputOption)));
	    for (FileStatus fileStatus : file_status) {
	    	if (fileStatus.isDir()) {
	    		runCSVCleanse(fileStatus.getPath(), new Path(cmdLine.getValue(csvOutputOption) + fileStatus.getPath().getName()));
	    	}
	    }
	    return 0;
 	}

	
	/**  runCSVCleanse - job runner - Clean pseudo CSV files
	 * @param none
	 * @return int - status of completion
	 * 
	 * */
	public int runCSVCleanse(Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();	
		Job job = new Job(conf);
		job.setJobName("Cleansing CSV Data for " + inputPath + " and placing it in " + outputPath);
		job.setJarByClass(MercyCSVCleanse.class);
		TextInputFormat.setInputPaths(job, inputPath);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapperClass(MercyCSVCleanseMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		job.setNumReduceTasks(0);
		if (cmdLine.hasOption(synchronous))
			return job.waitForCompletion(true) ? 0 : 1;
		job.submit();
		return 0;
	}
	/**
	 * 
	 * Main Class to Cleanse csv File.
	 * 
	 * @param args
	 * @throws Exception
	 */
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MercyCSVCleanse(), args);
		System.exit(exitCode);
	}
}
