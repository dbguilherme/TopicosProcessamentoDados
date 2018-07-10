package join_example;

//import java.io.IOException;
//import java.util.StringTokenizer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//public class WordCount {
//
//  public static class TokenizerMapper
//       extends Mapper<Object, Text, Text, IntWritable>{
//
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//
//    public void map(Object key, Text value, Context context
//                    ) throws IOException, InterruptedException {
//      StringTokenizer itr = new StringTokenizer(value.toString());
//      while (itr.hasMoreTokens()) {
//        word.set(itr.nextToken());
//        context.write(word, one);
//      }
//    }
//  }
//
//  public static class IntSumReducer
//       extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
//
//    public void reduce(Text key, Iterable<IntWritable> values,
//                       Context context
//                       ) throws IOException, InterruptedException {
//      int sum = 0;
//      for (IntWritable val : values) {
//        sum += val.get();
//      }
//      result.set(sum);
//      context.write(key, result);
//    }
//  }
//
//  public static void main(String[] args) throws Exception {
//    Configuration conf = new Configuration();
//    Job job = Job.getInstance(conf, "word count");
//    job.setJarByClass(WordCount.class);
//    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
//    job.setReducerClass(IntSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
//    FileInputFormat.addInputPath(job, new Path("teste.txt"));
//    FileOutputFormat.setOutputPath(job, new Path("out.txt"));
//    System.exit(job.waitForCompletion(true) ? 0 : 1);
//  }
//}
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {

  public static class LoadAge
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     
      String st[]= value.toString().split("::");
      int age = Integer.parseInt(st[2]);
      System.out.println(age);
      if(age>10)
    	  context.write(new Text(st[0]), value);

    }
  }  
  
  public static class LoadRatings extends Mapper<Object, Text, Text, Text>{

	  private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  
		  String st[]= value.toString().split("::");
		  int rate= Integer.parseInt(st[3]);
		  word.set(key.toString());
	      context.write(new Text(st[0]), new Text(Integer.toString(rate)));	      	    
	  }
  }


  public static class Reducing  extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
      String sum = "";//new Text();
      int count=0;
      for (Text val : values) {
    	  
        //sum	=sum.concat(val.toString());
        count++;
      }
      
      //result.set(sum);
      //System.out.println(result);
      context.write(key, new Text(Integer.toString(count)));
    }
  }

  public static void main(String[] args) throws Exception {
//    Configuration conf = new Configuration();
//    Job job = Job.getInstance(conf, "word count");
//    job.setJarByClass(WordCount.class);
//    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
//    job.setReducerClass(IntSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
//    FileInputFormat.addInputPath(job, new Path("/tmp/teste"));
//    FileOutputFormat.setOutputPath(job, new Path("/tmp/testeout"));
//    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	  Configuration conf = new Configuration();
	  Job job = new Job(conf, "Reduce-side join");
	  job.setJarByClass(ReduceJoin.class);
	  
	  Path p1=new Path("/tmp/users.dat");
	  Path p2=new Path("/tmp/ratings.dat");
	  
	  
	   
	  
	  FileSystem fs = FileSystem.get(conf);
	  
	  
	  MultipleInputs.addInputPath(job, p1,TextInputFormat.class, LoadAge.class);
	  
	  MultipleInputs.addInputPath(job, p2,TextInputFormat.class, LoadRatings.class);
	 // MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
	  Path outputPath = new Path("/tmp/testeou");
	  if(fs.exists(outputPath )){
		   fs.delete(outputPath , true);
	  }
	  
	  job.setReducerClass(Reducing.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  FileOutputFormat.setOutputPath(job, outputPath);
	  outputPath.getFileSystem(conf).delete(outputPath);
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	  
  }
}