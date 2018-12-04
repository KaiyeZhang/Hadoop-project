package task1;

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
public class SortMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, 
			Context context) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t");
		if(dataArray.length == 2) {
			context.write(new Text(dataArray[0]), new Text(dataArray[1]));
		}
	}
}
