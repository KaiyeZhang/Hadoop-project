package task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class cfReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		StringBuffer tag = new StringBuffer();
		String[] data1=key.toString().split("\t");
		String[] locationname=data1[0].split(",");
		for (Text val : values) {
            tag.append(val);
		}
		String[] tagArray=tag.toString().split(" ");
		String temp=tagfliter(locationname,tagArray);
		String temp1=tagfre(temp);
		
		context.write(new Text(key), new Text (temp1));
	}
	
	protected String tagfliter(String[] nameArray,String[] tagArray){
		ArrayList<String> placenames=new ArrayList<String>();
		ArrayList<String> tagnames=new ArrayList<String>();
		ArrayList<String> result=new ArrayList<String>();
		for(int i=0;i<nameArray.length;i++){
			placenames.add(nameArray[i].trim().replaceAll(",", "").replaceAll("\\s*",""));
		}
		for(int j=0;j<tagArray.length;j++){
			tagnames.add(tagArray[j].trim().replaceAll(",", "").replaceAll("\\s*", ""));
		}
		for(String name:placenames){
			for(String tag:tagnames){
				if(!(tag.equalsIgnoreCase(name))){
					result.add(tag+" ");
				}
			}
		}


	return result.toString();
	
   }
	
	protected String tagfre(String k){
	String[] temp =k.split(" ");
	Map<String, Integer> tagfreq = new HashMap<String,Integer>();
	for (String tag:temp){

	    Integer f = tagfreq.get(tag);
	    //checking null
	    if(f==null) f=0;
	    tagfreq.put(tag,f+1);
	}
	Map<String, Integer> sortedmap = sortByValue(tagfreq);
	StringBuffer result = new StringBuffer();
	int counter=0;
	
		for (Entry<String, Integer> entry : sortedmap.entrySet())
		{
			if(counter<10){
		    result.append("(" +entry.getKey() + ":" + entry.getValue()+")");
		    counter++;
			}
			else{
				break;
			}
		}
	
	
	return result.toString();
      
	
   }
	
    protected static <K, V extends Comparable<? super V>> Map<K, V> 
    sortByValue( Map<K, V> map ){
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>( map.entrySet() );
    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
    {
        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
        {
            return (o2.getValue()).compareTo( o1.getValue() );
        }
    } );

    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list)
    {
        result.put( entry.getKey(), entry.getValue() );
    }
    return result;
    }
	

}
