package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import task2.JoinDriver;
import task2.JoinGroupComparator;
import task2.JoinPartitioner;
import task2.JoinReducer;
import task2.PhotoMapper;
import task2.PlaceMapper;
import task2.TextIntPair;

public class tagmap50Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: tagmap50Driver <in> <in>");
			System.exit(2);
		}
		
		Path top50Table = new Path(otherArgs[0]);
		Path tagTable = new Path(otherArgs[1]);
		Path tempResult = new Path("tempResult2/tag50");
		
		Job tag50Job = new Job(conf, "join 50locality with numphoto and tag together");
//		joinJob.setNumReduceTasks(5); 
		tag50Job.setJarByClass(tagmap50Driver.class);
		
		MultipleInputs.addInputPath(tag50Job, top50Table, 
				TextInputFormat.class,top50Mapper.class);
		MultipleInputs.addInputPath(tag50Job, tagTable, TextInputFormat.class,tagMapper.class);
		tag50Job.setMapOutputKeyClass(TextIntPair.class);
		tag50Job.setMapOutputValueClass(Text.class);
		tag50Job.setOutputKeyClass(Text.class);
		tag50Job.setOutputValueClass(Text.class);
		tag50Job.setGroupingComparatorClass(JoinGroupComparator.class);
		tag50Job.setReducerClass(tag50Reducer.class);
		tag50Job.setPartitionerClass(JoinPartitioner.class);
		TextOutputFormat.setOutputPath(tag50Job, tempResult);
		tag50Job.waitForCompletion(true);
	}

}
