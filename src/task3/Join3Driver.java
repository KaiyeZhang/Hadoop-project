package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import task3.JoinGroupComparator;
import task3.JoinPartitioner;
import task3.Join3Reducer;
import task3.TextIntPair;

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
public class Join3Driver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: PlaceFilter <in> <in> <out>");
			System.exit(2);
		}
		
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);
		Path tempResult = new Path(otherArgs[2]);
		
		Job joinJob3 = new Job(conf, "join place photo");
//		joinJob.setNumReduceTasks(5); 
		joinJob3.setJarByClass(Join3Driver.class);
		
		MultipleInputs.addInputPath(joinJob3, photoTable, 
				TextInputFormat.class,placeid503Mapper.class);
		MultipleInputs.addInputPath(joinJob3, placeTable, TextInputFormat.class,Place3Mapper.class);
		joinJob3.setMapOutputKeyClass(task3.TextIntPair.class);
		joinJob3.setMapOutputValueClass(Text.class);
		joinJob3.setOutputKeyClass(Text.class);
		joinJob3.setOutputValueClass(Text.class);
		joinJob3.setGroupingComparatorClass(task3.JoinGroupComparator.class);
		joinJob3.setCombinerClass(JoinCombiner.class);
		joinJob3.setReducerClass(task3.Join3Reducer.class);
		joinJob3.setPartitionerClass(task3.JoinPartitioner.class);
		TextOutputFormat.setOutputPath(joinJob3, tempResult);
		joinJob3.waitForCompletion(true);
	}
}
