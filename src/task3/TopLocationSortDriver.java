package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopLocationSortDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: TopLocationSortDriver <in> <out> ");
			System.exit(2);
		}
		
		Job tlsJob = new Job(conf, "sort numOfPhotos at each location decreasingly");
		tlsJob.setJarByClass(TopLocationSortDriver.class);
		
//		tlsJob.setNumReduceTasks(3);
		tlsJob.setMapperClass(TopLocationSortMapper.class);
		tlsJob.setReducerClass(TopLocationSortReducer.class);
		tlsJob.setMapOutputKeyClass(IntWritable.class);
		tlsJob.setMapOutputValueClass(Text.class);
		tlsJob.setOutputKeyClass(Text.class);
		tlsJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(tlsJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(tlsJob, new Path(otherArgs[1]));
		tlsJob.waitForCompletion(true);
	}
}
