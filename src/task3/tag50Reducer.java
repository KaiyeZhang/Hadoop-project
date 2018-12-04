package task3;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class tag50Reducer extends Reducer<TextIntPair, Text, Text, Text> {
     
	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		    Iterator<Text> valuesItr = values.iterator();
		    StringBuffer tag = new StringBuffer();
		    if(key.getOrder().get()==0){
		    	while (valuesItr.hasNext()){
					tag.append(valuesItr.next().toString());
				}
//		    	String[] tagArray = tag.toString().split("\\s+");
//				String[] nameArray = key.getKey().toString().split(",");
//				String temp1=tagfliter(nameArray,tagArray);
//				String temp2=tagfre(temp1);
		    	context.write(new Text(key.getKey()), new Text (tag.toString()));
		    }
		    else{
		    	return;
		    }
	}
	
	
//	protected String tagfliter(String[] nameArray,String[] tagArray){
//		StringBuffer result = new StringBuffer();
//		String temp=placename(nameArray,tagArray);
//		String[] t=temp.split(" ");
//		for(int ii=0;ii<tagArray.length;ii++){
//			for(int jj=0;jj<t.length;jj++){
//				if(!(String.valueOf(tagArray[ii].trim()).equalsIgnoreCase(String.valueOf(t[jj])))&&!(tagArray[ii].compareTo(t[jj])==0)){
//					result.append(tagArray[ii]+" ");
//					break;
//				}
//				else{
//					break;
//				}
//			}
//		}

//		for(int i=1;i<tagArray.length;i++){
//			for(int j=0;j<nameArray.length;j++){
//              if(!(nameArray[j].trim().replaceAll(" ", "").toLowerCase().contains(tagArray[i].trim().toLowerCase()))&&
//						!(String.valueOf(nameArray[j].trim()).equalsIgnoreCase(String.valueOf(tagArray[i].trim())))&&!isNumeric(tagArray[i])){
//							result.append(tagArray[i]+" ");
//					
//				}
//			}
//		}
//		return result.toString();
//		
//	}
//	
//	protected String placename (String[] nameArray,String[] tagArray){
//		StringBuffer result = new StringBuffer();
//		String temp=processpn(nameArray);
//		String[] temp2=temp.split(" ");
//		for(int i=0;i<tagArray.length;i++){
//			for(int j=0;j<temp2.length;j++){
//				if((String.valueOf(temp2[j].trim()).equalsIgnoreCase(String.valueOf(tagArray[i].trim())))){
//							result.append(tagArray[i]+" ");
//							break;
//					
//				}
//				else{
//					break;
//				}
//			}
//		}
//		return result.toString();
//		
//	}
//	protected String processpn (String[] nameArray){
//		StringBuffer result = new StringBuffer();
//	    for(int i=0;i<nameArray.length;i++){
//	    	result.append(nameArray[i].trim().replaceAll(" ", "").toLowerCase()+" ");
//	    }
//		return result.toString();
//		
//	}
//	public static boolean stringCompare(String newData, String intialData) {
//		if (newData != intialData) {
//			if (newData == null || intialData == null)
//				return false;
//			return newData.equals(intialData);
//		}
//		return true;
//	}
// 
//	
//	protected static boolean isNumeric(String str)  
//	{  
//	  try  
//	  {  
//	    double d = Double.parseDouble(str);  
//	  }  
//	  catch(NumberFormatException nfe)  
//	  {  
//	    return false;  
//	  }  
//	  return true;  
//	}
//	
//	protected String tagfre(String k){
//	String[] temp =k.split("\\s+");
//	Map<String, Integer> tagfreq = new HashMap<String,Integer>();
//	for (String tag:temp){
//
//	    Integer f = tagfreq.get(tag);
//	    //checking null
//	    if(f==null) f=0;
//	    tagfreq.put(tag,f+1);
//	}
//	Map<String, Integer> sortedmap = sortByValue(tagfreq);
//	StringBuffer result = new StringBuffer();
//	int counter=0;
//	
//		for (Entry<String, Integer> entry : sortedmap.entrySet())
//		{
//			if(counter<10){
//		    result.append("(" +entry.getKey() + ":" + entry.getValue()+")");
//		    counter++;
//			}
//			else{
//				break;
//			}
//		}
//	
//	
//	return result.toString();
//      
//	
//   }
//	
//    protected static <K, V extends Comparable<? super V>> Map<K, V> 
//    sortByValue( Map<K, V> map ){
//    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>( map.entrySet() );
//    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
//    {
//        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
//        {
//            return (o2.getValue()).compareTo( o1.getValue() );
//        }
//    } );
//
//    Map<K, V> result = new LinkedHashMap<K, V>();
//    for (Map.Entry<K, V> entry : list)
//    {
//        result.put( entry.getKey(), entry.getValue() );
//    }
//    return result;
//    }


	
}
