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

import task2.JoinGroupComparator;
import task2.JoinPartitioner;
import task2.TextIntPair;

public class tagfreDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: tagmap50Driver <in>");
			System.exit(2);
		}
		
		Path lastresult = new Path(otherArgs[0]);
		Path tempResult = new Path("tempResult2/testfinal");
		
		Job tagfreJob = new Job(conf, "The final result with localityname ,numphoto, tags and their frequence");
//		joinJob.setNumReduceTasks(5); 
		tagfreJob.setJarByClass(tagfreDriver.class);
		tagfreJob.setMapperClass(tagfreMapper.class);
		tagfreJob.setMapOutputKeyClass(Text.class);
		tagfreJob.setMapOutputValueClass(Text.class);
		tagfreJob.setOutputKeyClass(Text.class);
		tagfreJob.setOutputValueClass(Text.class);
		tagfreJob.setReducerClass(tagfreReducer.class);
		TextInputFormat.addInputPath(tagfreJob, lastresult);
		TextOutputFormat.setOutputPath(tagfreJob, tempResult);
		tagfreJob.waitForCompletion(true);
	}

}
