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
public class tagMapper extends Mapper<Object, Text, TextIntPair, Text> {

	public void map(Object key, Text value, 
			Context context) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t");
		String tag=fliternum(dataArray[2]);
		if(dataArray.length == 6) {
			context.write(new TextIntPair(dataArray[4],1), new Text(tag));
		}
	}
	
	protected String fliternum(String string){
		String[] k=string.split(" ");
		StringBuffer result = new StringBuffer();
		for(int i=0;i<k.length;i++){
			if(!(isNumeric(k[i].trim()))){
				result.append(k[i]+" ");
			}
		}
		return result.toString();
		
	}
	
	protected static boolean isNumeric(String str)  
	{  
		  try  
		  {  
		    double d = Double.parseDouble(str);  
		  }  
		  catch(NumberFormatException nfe)  
		  {  
		    return false;  
		  }  
		  return true;  
		}
}

