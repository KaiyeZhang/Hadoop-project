package task2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This mapper is used to filter place names containing a particular country name
 * 
 * input format:
 * place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
 * 
 * output format:
 * place_id \t place_url
 * 
 * The country name is stored as a property in the job's configuration object.
 * 
 * The configuration object can be obtained from the mapper/reducer's context object
 * 
 * Because all calls to the map function needs to use the country name value, we save it
 * as an instance variable countryName and set the value of it in the setup method. 
 * The setup method is called after the mapper is created. It is before any call of the
 * first map method. 
 * 
 * @see idphotoDriver
 * @author Kaiye Zhang
 */


public class SortCombiner extends Reducer<Text, Text, Text,  Text> {
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

         
		int sum=0;
		for(Text value:values){
			sum+=Integer.parseInt(value.toString());
		}
		context.write(key, new Text(""+sum));
	}
}

