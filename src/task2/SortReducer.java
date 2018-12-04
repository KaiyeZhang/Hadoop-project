package task2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class SortReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		int numOfPhotos = 0;
		for(Text val : values) {
			numOfPhotos += Integer.parseInt(val.toString());
		}
		context.write(key, new Text("" + numOfPhotos));
	}
}
