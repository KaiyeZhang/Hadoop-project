package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class cfCombiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuffer tag = new StringBuffer();
		for(Text value:values){
			tag.append(value);
		}
		context.write(key, new Text(tag.toString()+" "));
	}

}
