package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class TopLocationSortReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
	
	private static int numOfRecords = 0;
	IntWritable reverseKey = new IntWritable();
	//just reverse the key and value again!
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		if(numOfRecords < 50){
			reverseKey.set(-1 * key.get());
			for (Text val : values) {
				context.write(val, reverseKey);
				numOfRecords++;
			}
		}
	}
}