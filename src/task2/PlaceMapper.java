package task2;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * The mapper to read place data
 * 
 * For testing, using place.txt as the photo table 
 * place.txt has the format:
 * place_id \t woeid \t longitude \t latitude \t place name \t place_type_id \t place_url
 * 
 * @author Ying  Zhou
 *
 */
public class PlaceMapper extends  Mapper<Object, Text, TextIntPair, Text> {
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >=6) {
			int typeId = Integer.parseInt(dataArray[5]);
			String placeId = dataArray[0];
			if(typeId == 22) {
				String locationName = praseloalityname(dataArray[4]);
				context.write(new TextIntPair(placeId,0), new Text(locationName));
			}
			else if(typeId == 7) {
				String locationName = dataArray[4];
				context.write(new TextIntPair(placeId,0), new Text(locationName));
			}
		}
	}
	
	protected String praseloalityname(String name){
		String[] fullname=name.split(",");
		StringBuffer lname = new StringBuffer();
		for(int i=1;i<fullname.length;i++){
			if(i!=fullname.length-1){
				lname.append(fullname[i]+",");
			}
			else{
				lname.append(fullname[i]);
			}
			
		}
		String str=lname.toString();
		return str;
	}
}


