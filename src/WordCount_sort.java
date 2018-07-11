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
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount_sort {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  
  public static class SortMap  extends Mapper<Object, Text, IntWritable, Text>{
	  private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();

	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
		  String line[] = value.toString().split("\t");
		  
		  
		 context.write(new IntWritable(Integer.parseInt(line[1])), new Text (line[0]));
			  
		  
		  
	  }
  }

  public static class SortReduce   extends Reducer<IntWritable, Text,IntWritable, Text> {
	  private IntWritable result = new IntWritable();

	  public void reduce(IntWritable key, Iterable<Text> values,   Context context  ) throws IOException, InterruptedException {
		  int sum = 0;
		  
		  for (Text val : values) {
			  context.write(key, val);
		  }
		  
		  
	  }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount_sort.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setReducerClass(IntSumReducer.class);
   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    Path in = new Path("DATA/words.dat");
    Path out = new Path("out5");
    FileSystem fs = FileSystem.get(conf);
    
	if(fs.exists(out)){
		   fs.delete(out, true);
	}
    
   // job.setInputFormatClass(TextInputFormat.class);
   // job.setOutputFormatClass(TextOutputFormat.class);
         
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);
    job.waitForCompletion(true);
    
    //////////////////////
    Job jobB = Job.getInstance(conf, "sort");
    jobB.setJarByClass(WordCount_sort.class);
    jobB.setMapperClass(SortMap.class);

    jobB.setReducerClass(SortReduce.class);
   
    jobB.setOutputKeyClass(IntWritable.class);
    jobB.setOutputValueClass(Text.class);
    
    in = new Path("out5/part-r-00000");
    out = new Path("out6");
   fs = FileSystem.get(conf);
    
	if(fs.exists(out)){
		   fs.delete(out, true);
	}
    
   // job.setInputFormatClass(TextInputFormat.class);
   // job.setOutputFormatClass(TextOutputFormat.class);
         
    FileInputFormat.addInputPath(jobB, in);
    FileOutputFormat.setOutputPath(jobB, out);
    jobB.waitForCompletion(true);

    
  }
}