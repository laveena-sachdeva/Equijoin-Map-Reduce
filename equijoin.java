import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;
import java.util.Iterator;
import java.util.ArrayList;


public class equijoin
{
	
//Mapper Class	
	public static class EquiJoinMapper extends Mapper<Object, Text, Text, Text> {
	     
		private Text joinKey = new Text();
	    private Text outputValue = new Text();
//Implementing the map method	
		@Override
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//Split the input row on ',' to fetch the value of join column	    	
			String inputRow[] = value.toString().split(",");

//Fetching the join column			
			String jKey = inputRow[1];

//Set the value of join	column as the key in the output	
	    	joinKey.set(jKey);
//The value in the output would be the complete record			
	    	outputValue.set(value);
//write the output of the mapper	        	      
	    	context.write(joinKey,outputValue);
	    	
	     }
	   }

//Reducer class	   
	   public static class EquiJoinReducer extends Reducer<Text, Text, Text, Text> {

//Implementing the reduce method	   
		   @Override
		   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	           
			   String r1="";
			   String r2="";
//Creating separate lists for separate relations
			   ArrayList<String>  relation1 = new ArrayList<String>();
			   ArrayList<String>  relation2 = new ArrayList<String>();
			   
			   Text outputValue = new Text();
			   String joinedtuple = "";
			   
//Iterate over the list of values corresponding to a particular key to separate the records
			   for(Text value: values)
			   { 
				  
				   String r = value.toString();
				   String r_columns[] = r.split(",");
				   
				   if(r1.equals(""))
				   {
					   r1=r_columns[0];
				   }
				   else if(!r1.equals("") && !r1.equals(r_columns[0]) && r2.equals(""))
				   {
					   r2=r_columns[0];
				   }
				 //checks for the joining key and add it separately in a table
				 
				 if(r_columns[0].equals(r1)){
					   relation1.add(r);
				   }
				   else if(r_columns[0].equals(r2)){
					   relation2.add(r);
				   }
			   }	        	       	       	        	        	     
	        
			
//If one relation has no matching key in the other realtion ignore
			   if(relation2.size() == 0 || relation1.size() ==0){
				  
			   }
//Otherwise, join each record of one relation with other relation anad write the output value			   
			   else{
				   for(int i=0; i<relation1.size(); i++){
					   for(int j=0;j<relation2.size();j++){ 
							   joinedtuple= relation1.get(i) + "," + relation2.get(j);
							   outputValue.set(joinedtuple);
							   context.write(new Text(""), outputValue);	        		
					   }
				   }
			   }	         	         	       	        	        	         	        
		   }
	   }
//Driver
	   public static void main(String[] args) throws Exception {	       	 
	       
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//set the mapper and reducer class		
			Job job = new Job(conf, "EquiJoin");
			job.setJarByClass(equijoin.class);
			job.setMapperClass(EquiJoinMapper.class);
			job.setReducerClass(EquiJoinReducer.class);
//set the output key-value classes		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);	
//set the input and output file path and start the job			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

	   }
}
