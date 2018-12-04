package task3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import task3.TextIntPair;

/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class Join3Reducer extends  Reducer<TextIntPair, Text, Text, Text> {

	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//check if the key is coming from the place table
		Iterator<Text> valuesItr = values.iterator();
		StringBuffer locationinfo = new StringBuffer();
		if (key.getOrder().get() == 0){// the key is from the place table
			String  placeid= valuesItr.next().toString();
			while (valuesItr.hasNext()){
				locationinfo.append(valuesItr.next().toString());
			}
			context.write(new Text(placeid.replaceAll("[\\p{C}]", " ").trim()), new Text (""+locationinfo));
		}else{ // the key is not from the place table, but the photo table
			while(valuesItr.hasNext()){
				valuesItr.next();
			}
		}	
	}
}


