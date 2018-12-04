package task3;

import java.io.IOException;


import java.io.IOException;
import java.util.*;

import org.apache.commons.collections.map.StaticBucketMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class tagfreMapper extends Mapper<Object, Text, IntWritable, Text> {
	private IntWritable numOfPhotos = new IntWritable();
	public void map(Object key, Text value, 
			Context context) throws IOException, InterruptedException {
		String[] valArray = value.toString().split("\t");
		
		int num=(-1)*Integer.parseInt(valArray[0].trim());
		StringBuffer tag = new StringBuffer();
		numOfPhotos.set(num);
        for(int i=2;i<valArray.length;i++){
        	tag.append(valArray[i]+" ");
        }
		context.write(numOfPhotos, new Text(valArray[1]+";"+tag.toString()));

	}
	

}
