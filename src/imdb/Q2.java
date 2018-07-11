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

/*Q2= Encontrar os id dos usuários que avaliaram mais de 10 filmes (usar  o aquivos ratings.dat)

 */
public class Q2 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		
		private static final IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] currentUserTuples=line.split(":");
			
			context.write( new Text (currentUserTuples[0]), value);
			
//			int currentAgeValue = Integer.parseInt(currentUserTuples[2].trim());
//			if(currentUserTuples[1].trim().equalsIgnoreCase("M")&&currentAgeValue<=targetAge)
//			{
//				context.write(new Text("UserId :: "+currentUserTuples[0].trim()), one);
//			}			
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>
	{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int count=0;
			for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
				Text value = iterator.next();
				count++;
			}
			if(count>100) {
				context.write(key, new IntWritable(count));
						
			}
			
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
	    Job job = new Job(conf, "Q2");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setJarByClass(Q2.class);
	    job.setMapperClass(Map.class);
	   
	    job.setReducerClass(Reduce.class);
	    
	    
	    
	    Path in = new Path("DATA/ratings.dat");
	    Path out = new Path("out5");
	    FileSystem fs = FileSystem.get(conf);
	    
		if(fs.exists(out)){
			   fs.delete(out, true);
		}
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	         
	    FileInputFormat.addInputPath(job, in);
	    FileOutputFormat.setOutputPath(job, out);
	    
	    
	    
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