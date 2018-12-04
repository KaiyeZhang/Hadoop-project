package task3;

import java.io.IOException;
import java.util.*;

import org.apache.commons.collections.map.StaticBucketMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class top50Mapper extends Mapper<Object, Text, TextIntPair, Text> {

	public void map(Object key, Text value, 
			Context context) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t");
		if(dataArray.length == 3) {
			context.write(new TextIntPair(dataArray[0],0), new Text(dataArray[1]+"\t"+dataArray[2]+"\t"));
		}
	}
}
