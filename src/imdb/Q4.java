package imdb;



import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*Q3= Encontrar as ocupações dos usuários com mais de 60 anos. (usar o user.dat)

 */
public class Q4 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] currentUserTuples=line.split(":");
			String keyStr = currentUserTuples[1].trim()+currentUserTuples[2].trim();
			context.write(new Text(keyStr), one);		
			
		}

	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//No. of movies  rated n
		int num;

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value : values)
			{
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration(); 
	    //conf.set("inParameter", args[0]);
	    Job job = new Job(conf, "Q3");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setJarByClass(Q4.class);
	    job.setMapperClass(Map.class);
	   
	    job.setReducerClass(Reduce.class);
	    
	    
	    
	    Path in = new Path("DATA/users.dat");
	    Path out = new Path("out5");
	    FileSystem fs = FileSystem.get(conf);
	    
		if(fs.exists(out)){
			   fs.delete(out, true);
		}
	    
		//job.setMapOutputKeyClass(TextInputFormat.class);
		//job.setRed
		
		
	   // job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(IntWritable.class);
	         
	    FileInputFormat.addInputPath(job, in);
	    FileOutputFormat.setOutputPath(job, out);
	    
	    
	    
	    job.waitForCompletion(true);
	}
	
	private static String toString(Text[] list)
	{
		String ret="";
		for(int i=2;i<list.length;i++)
		{
			ret=ret+list[i]+" ";
		}
		return ret;
	}

}