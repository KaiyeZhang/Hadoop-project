package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is a sample program to configure a map only job to
 * filter place data based on input countryName
 * 
 * The country name is passed as a command line argument and is 
 * stored as a property in the job's configuration object. 
 * 
 * The property "mapper.placeFilter.country" can be read out
 * by both Mapper and Reducer through their context objects.
 * 
 * @see PlaceFilterMapper
 * @author Ying Zhou
 *
 */
public class JoinDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: PlaceFilter <in> <in>");
			System.exit(2);
		}
		
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);
		Path tempResult = new Path("tempResult2");
		
		Job joinJob = new Job(conf, "join place photo");
//		joinJob.setNumReduceTasks(5); 
		joinJob.setJarByClass(JoinDriver.class);
		
		MultipleInputs.addInputPath(joinJob, photoTable, 
				TextInputFormat.class,PhotoMapper.class);
		MultipleInputs.addInputPath(joinJob, placeTable, TextInputFormat.class,PlaceMapper.class);
		joinJob.setMapOutputKeyClass(TextIntPair.class);
		joinJob.setMapOutputValueClass(Text.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		joinJob.setGroupingComparatorClass(JoinGroupComparator.class);
		joinJob.setReducerClass(JoinReducer.class);
		joinJob.setPartitionerClass(JoinPartitioner.class);
		TextOutputFormat.setOutputPath(joinJob, tempResult);
		joinJob.waitForCompletion(true);
	}
}
