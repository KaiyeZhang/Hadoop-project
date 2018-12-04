package task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopLocationSortMapper extends Mapper<Object, Text, IntWritable, Text> {
	private Text location = new Text();
	private IntWritable numOfPhotos = new IntWritable();
	//just reverse the key and value
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] dataArray = value.toString().split("\t");

		if (dataArray.length == 2){
				try{
					location.set(dataArray[0]);
					numOfPhotos.set(-1 * Integer.parseInt(dataArray[1]));
					context.write(numOfPhotos, location);
				}catch(Exception e){
					System.out.println("Mapper");
					e.printStackTrace(); //not doing anything
				}
		}
	}
}
