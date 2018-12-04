package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import task2.JoinDriver;
import task2.JoinGroupComparator;
import task2.JoinPartitioner;
import task2.JoinReducer;
import task2.TextIntPair;


public class Chain3Driver {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: JoinDriver <in1> <in2> <out>");
			System.exit(2);
		}
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);
		Path tempResult = new Path("tempResult/test3");
		Path tempResultsorttemp=new Path("tempResult/sorttemp3");
		Path tempResultplaceid=new Path("tempResult/tagtemp3");
		Path tempResulttop50=new Path("tempResult/top50temp4");
		Path tempResulttop50mix=new Path("tempResult/top50temp2");
		Path tempResulttop50tag=new Path("tempResult/toptagtemp3");
		Path outputTable = new Path(otherArgs[2]);
		
		
		Job joinJob = new Job(conf, "join place photo");
//		joinJob.setNumReduceTasks(5); 
		joinJob.setJarByClass(task3.JoinDriver.class);
		MultipleInputs.addInputPath(joinJob, photoTable, 
				TextInputFormat.class,task3.PhotoMapper.class);
		MultipleInputs.addInputPath(joinJob, placeTable, TextInputFormat.class,task3.PlaceMapper.class);
		joinJob.setMapOutputKeyClass(task3.TextIntPair.class);
		joinJob.setMapOutputValueClass(Text.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		joinJob.setGroupingComparatorClass(task3.JoinGroupComparator.class);
		joinJob.setReducerClass(task3.JoinReducer.class);
		joinJob.setPartitionerClass(task3.JoinPartitioner.class);
		TextOutputFormat.setOutputPath(joinJob, tempResult);
		joinJob.waitForCompletion(true);
		
		Job sortJob = new Job(conf, "sort locationName alphabetically");
		sortJob.setJarByClass(task3.SortDriver.class);
//		sortJob.setNumReduceTasks(1);
		sortJob.setMapperClass(task3.SortMapper.class);
		sortJob.setCombinerClass(task3.SortCombiner.class);
		sortJob.setReducerClass(task3.SortReducer.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(sortJob, tempResult);
		TextOutputFormat.setOutputPath(sortJob, tempResultsorttemp);
		sortJob.waitForCompletion(true);
		FileSystem.get(conf).delete(tempResult, true);
		
		Job tlsJob = new Job(conf, "sort numOfPhotos at each location decreasingly");
		tlsJob.setJarByClass(task3.TopLocationSortDriver.class);
//		tlsJob.setNumReduceTasks(3);
		tlsJob.setMapperClass(task3.TopLocationSortMapper.class);
		tlsJob.setReducerClass(task3.TopLocationSortReducer.class);
		tlsJob.setMapOutputKeyClass(IntWritable.class);
		tlsJob.setMapOutputValueClass(Text.class);
		tlsJob.setOutputKeyClass(Text.class);
		tlsJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(tlsJob, tempResultsorttemp);
		TextOutputFormat.setOutputPath(tlsJob,tempResulttop50);
		tlsJob.waitForCompletion(true);
		FileSystem.get(conf).delete(tempResultsorttemp, true);

		Job joinJob3 = new Job(conf, "extract placeid with locationname and num photos, output<(placeid,0),(locationname,numphotos)>");
		joinJob3.setNumReduceTasks(0); 
		
		joinJob3.addCacheFile(new Path("tempResult/top50temp4/part-r-00000").toUri());
		joinJob3.setJarByClass(Join3Driver.class);
//		MultipleInputs.addInputPath(joinJob3, tempResulttop50, 
//				TextInputFormat.class,placeid503Mapper.class);
		MultipleInputs.addInputPath(joinJob3, placeTable, TextInputFormat.class,Place3Mapper.class);
		//joinJob3.setMapOutputKeyClass(task3.TextIntPair.class);
		//joinJob3.setMapOutputValueClass(Text.class);
		joinJob3.setOutputKeyClass(Text.class);
		joinJob3.setOutputValueClass(Text.class);
		//joinJob3.setGroupingComparatorClass(task3.JoinGroupComparator.class);
		//joinJob3.setCombinerClass(task3.JoinCombiner.class);
		//joinJob3.setReducerClass(task3.Join3Reducer.class);
		//joinJob3.setPartitionerClass(task3.JoinPartitioner.class);
		TextOutputFormat.setOutputPath(joinJob3, tempResultplaceid);
		joinJob3.waitForCompletion(true);
		FileSystem.get(conf).delete(tempResulttop50, true);
		
		Job tag50Job = new Job(conf, "join 50locality with numphoto and tag together");
        tag50Job.setJarByClass(tagmap50Driver.class);
		MultipleInputs.addInputPath(tag50Job, tempResultplaceid, 
				TextInputFormat.class,task3.top50Mapper.class);
		MultipleInputs.addInputPath(tag50Job, photoTable, TextInputFormat.class,task3.tagMapper.class);
		tag50Job.setMapOutputKeyClass(task3.TextIntPair.class);
		tag50Job.setMapOutputValueClass(Text.class);
		tag50Job.setOutputKeyClass(Text.class);
		tag50Job.setOutputValueClass(Text.class);
		tag50Job.setGroupingComparatorClass(task3.JoinGroupComparator.class);
		//joinJob.setNumReduceTasks(3); 
		tag50Job.setReducerClass(task3.tag50Reducer.class);
		tag50Job.setPartitionerClass(task3.JoinPartitioner.class);
		TextOutputFormat.setOutputPath(tag50Job, tempResulttop50mix);
		tag50Job.waitForCompletion(true);
		FileSystem.get(conf).delete(tempResultplaceid, true);
		
		Job tagfre50Job = new Job(conf, "count fre of tags");
		tagfre50Job.setJarByClass(cfDriver.class);
		//tagfre50Job.setNumReduceTasks(0); 
		MultipleInputs.addInputPath(tagfre50Job, tempResulttop50mix, TextInputFormat.class,cfMapper.class);
		tagfre50Job.setMapOutputKeyClass(Text.class);
		tagfre50Job.setMapOutputValueClass(Text.class);
		tagfre50Job.setOutputKeyClass(Text.class);
		tagfre50Job.setOutputValueClass(Text.class);
		tagfre50Job.setCombinerClass(task3.cfCombiner.class);
		tagfre50Job.setReducerClass(cfReducer.class);
		TextOutputFormat.setOutputPath(tagfre50Job, outputTable);
		tagfre50Job.waitForCompletion(true);
//		
//		Job tagfreJob = new Job(conf, "The final result with localityname ,numphoto, tags and their frequence");
//		//joinJob.setNumReduceTasks(1); 
//		tagfreJob.setJarByClass(tagfreDriver.class);
//		tagfreJob.setMapperClass(tagfreMapper.class);
//		tagfreJob.setMapOutputKeyClass(IntWritable.class);
//		tagfreJob.setMapOutputValueClass(Text.class);
//		tagfreJob.setOutputKeyClass(Text.class);
//		tagfreJob.setOutputValueClass(Text.class);
//		tagfreJob.setPartitionerClass(task3.JoinPartitioner.class);
//		tagfreJob.setReducerClass(tagfreReducer.class);
//		TextInputFormat.addInputPath(tagfreJob, tempResulttop50tag);
//		TextOutputFormat.setOutputPath(tagfreJob, outputTable);
//		tagfreJob.waitForCompletion(true);
//		FileSystem.get(conf).delete(tempResulttop50tag, true);
	    
	}
		
}
	