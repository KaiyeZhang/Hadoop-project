package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ChainDriver {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: JoinDriver <in1> <in2> <out>");
			System.exit(2);
		}
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);
		Path tempResult = new Path("tempResult/test1");
		Path tempResult50=new Path("tempResult/test50");
		Path outputTable = new Path(otherArgs[2]);
		
		
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
		
		Job sortJob = new Job(conf, "sort locationName alphabetically");
		sortJob.setJarByClass(SortDriver.class);
//		sortJob.setNumReduceTasks(1);
		sortJob.setMapperClass(SortMapper.class);
		sortJob.setCombinerClass(SortCombiner.class);
		sortJob.setReducerClass(SortReducer.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(sortJob, tempResult);
		TextOutputFormat.setOutputPath(sortJob, tempResult50);
		sortJob.waitForCompletion(true);
		FileSystem.get(conf).delete(tempResult, true);
		
		Job tlsJob = new Job(conf, "sort numOfPhotos at each location decreasingly");
		tlsJob.setJarByClass(TopLocationSortDriver.class);
//		tlsJob.setNumReduceTasks(3);
		tlsJob.setMapperClass(TopLocationSortMapper.class);
		tlsJob.setReducerClass(TopLocationSortReducer.class);
		tlsJob.setMapOutputKeyClass(IntWritable.class);
		tlsJob.setMapOutputValueClass(Text.class);
		tlsJob.setOutputKeyClass(Text.class);
		tlsJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(tlsJob, tempResult50);
		TextOutputFormat.setOutputPath(tlsJob,outputTable);
		tlsJob.waitForCompletion(true);
		FileSystem.get(conf).delete(tempResult50, true);
	    
	}
		
}
	