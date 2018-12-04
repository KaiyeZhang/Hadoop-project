package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import task3.TextIntPair;


/**
 * The mapper to read photo
 * 
 * using n05.txt as the photo table 
 * n05.txt's format is:
 * photo_id \t owner \t tags \t date_taken\t place_id \t accuracy
 * 
 * output: place_id, user \t date
 * @author ying Zhou
 *
 */
public class placeid503Mapper extends Mapper<Object, Text, TextIntPair, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		
		if (dataArray.length==2) {
			String placename = dataArray[0];
			String photonum = dataArray[1];
			context.write(new TextIntPair(placename,0), new Text(photonum));
		}
		
	}

}
