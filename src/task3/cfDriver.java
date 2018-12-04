package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class cfDriver {

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
		
		Job tagfre50Job = new Job(conf, "count fre of tags");
		tagfre50Job.setJarByClass(cfDriver.class);
		
		MultipleInputs.addInputPath(tagfre50Job, photoTable, 
				TextInputFormat.class,PhotoMapper.class);
		MultipleInputs.addInputPath(tagfre50Job, placeTable, TextInputFormat.class,PlaceMapper.class);
		tagfre50Job.setMapOutputKeyClass(Text.class);
		tagfre50Job.setMapOutputValueClass(Text.class);
		tagfre50Job.setOutputKeyClass(Text.class);
		tagfre50Job.setOutputValueClass(Text.class);
		tagfre50Job.setGroupingComparatorClass(JoinGroupComparator.class);
		tagfre50Job.setReducerClass(JoinReducer.class);
		tagfre50Job.setPartitionerClass(JoinPartitioner.class);
		TextOutputFormat.setOutputPath(tagfre50Job, tempResult);
		tagfre50Job.waitForCompletion(true);
	}

}
