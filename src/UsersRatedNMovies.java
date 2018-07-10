/**
 * 
 */


import java.io.File;
import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author Dinesh Appavoo
 * 
 * Q1. Find all the user ids who has rated at least n movies. (n=30 or 40) [use only ratings.dat as input file]
 *
 */
public class UsersRatedNMovies {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] s=line.split("::");
			context.write(new Text(s[0]), one);
			//System.out.println(s[0]);
		}

	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//No. of movies  rated n
		int num;
		@Override
		protected void setup(Context context)
		{
			Configuration config=context.getConfiguration();
			//num=Integer.parseInt(config.get("inParameter").toString().trim());
			num=50;
			
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value : values)
			{
				sum+=value.get();
			}
			
			if(sum>num){
				context.write(key, new IntWritable(sum));
				System.out.println(key + " "+(sum));
			}
		}
	}	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration(); 
	    conf.set("inParameter", toString(args));
	    Job job = new Job(conf, "wordcount");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(UsersRatedNMovies.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	         
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	       
	    
	    File index = new File("/tmp/out7");
	    try{
		    String[] entries = index.list();
		    for(String s: entries){
		        File currentFile = new File(index.getPath(),s);
		        currentFile.delete();
		    }
		  }
		catch (Exception e){
			System.out.println("");
		}
	    index.delete();
	    
	    
	    
	    FileInputFormat.addInputPath(job, new Path("/tmp/ratings.dat"));
	    FileOutputFormat.setOutputPath(job, new Path("/tmp/out7"));
	         
	    job.waitForCompletion(true);
		

	}
	private static String toString(String[] list)
	{
		String ret="";
		for(int i=2;i<list.length;i++)
		{
			ret=ret+list[i]+" ";
		}
		return ret;
	}
	
	

}
