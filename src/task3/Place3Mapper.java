package task3;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import task3.TextIntPair;
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
public class Place3Mapper extends  Mapper<Object, Text, Text, Text> {
	
	private Hashtable <String, String> placenumTable = new Hashtable<String, String>();
	public void setup(Context context)
			throws java.io.IOException, InterruptedException{
			
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (cacheFiles != null && cacheFiles.length > 0) {
				String line;
				String[] placenumArray;
				BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					while ((line = placeReader.readLine()) != null) {
						placenumArray = line.split("\t");
						if(placenumArray.length==2){
							placenumTable.put(placenumArray[0],placenumArray[1]);
					
						}
						else{
							return;
						}
					}
					//System.out.println("size of the place table is: " + placeTable.size());
				} 
				finally {
					placeReader.close();
				}
			}
		}
	
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >=6) {
			int typeId = Integer.parseInt(dataArray[5]);
			String placeId = dataArray[0];
			if(typeId == 22) {
				String locationName = praseloalityname(dataArray[4]).trim();
				for (String key1 : placenumTable.keySet()) {
					if(locationName.equals(key1)){
						context.write(new Text(placeId),new Text(locationName+"\t"+placenumTable.get(key1)));
					}
				}
				
			}
			else if(typeId == 7) {
				String locationName = dataArray[4].trim();
				for (String key1 : placenumTable.keySet()) {
					if(locationName.equals(key1)){
						context.write(new Text(placeId),new Text(locationName+"\t"+placenumTable.get(key1)));
					}
				}
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



