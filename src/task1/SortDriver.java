package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: ReplicationJoinDriver <inPlace> <inPhoto> <out> ");
			System.exit(2);
		}
		Path tempResult = new Path("tempResult");
		Path outputTable = new Path(otherArgs[2]);
		
		Job sortJob = new Job(conf, "sort locationName alphabetically");
		sortJob.setJarByClass(SortDriver.class);
		
//		sortJob.setNumReduceTasks(1);
		sortJob.setMapperClass(SortMapper.class);
		sortJob.setReducerClass(SortReducer.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(sortJob, tempResult);
		TextOutputFormat.setOutputPath(sortJob, outputTable);
		sortJob.waitForCompletion(true);
	}
}
