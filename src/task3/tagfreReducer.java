package task3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class tagfreReducer extends Reducer< IntWritable, Text, Text, Text> {
	IntWritable reverseKey = new IntWritable();
	public void reduce(IntWritable key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		StringBuffer tagtemp = new StringBuffer();
		StringBuffer tagtemp1 = new StringBuffer();
		reverseKey.set((-1)*key.get());
		Iterator<Text> valuesItr = values.iterator();
        if(valuesItr.hasNext()){
        	tagtemp.append(valuesItr.next());
        }
        String[]nametemp=tagtemp.toString().split(";");
		String lname=nametemp[0];
//		for(int i=1;i<nametemp.length;i++){
//			tagtemp1.append(nametemp[i]+" ");
//		}
		//String tag = tagtemp1.toString();
        context.write(new Text(lname.split(",")[0]+"\t"),new Text(reverseKey+"\t"+nametemp[1]));	
		}
		  	
}

