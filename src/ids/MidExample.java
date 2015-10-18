package ids;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MidExample {
	
	public static void Main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "mid sampler");
		
		job.setJarByClass(MidExample.class);
		
		job.setMapperClass(MidExampleMapper.class);
		job.setReducerClass(MidExampleReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	public static class MidExampleMapper 
		extends Mapper<LongWritable,Text,IntWritable,Text>{
		
		private IntWritable orderId = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    int count = 0;
		      
		     while (tokenizer.hasMoreTokens()){
		    	 if(count == 1)
		    		 orderId.set(Integer.parseInt(tokenizer.nextToken()));
		    	 count++;
		     }// while loop
		     context.write(orderId, new Text(line));
		}// map
	}// mapper
	
	public static class MidExampleReducer
		extends Reducer<Text,Text,IntWritable,Text>{
		
		private String output = new String();
		
		public void reduce(IntWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException{
			
			while (values.hasNext())
		    	output = output + " " + values.next();
		    
			context.write(key, new Text(output));
		    
		}// reduce
	}// reducer
	
}// class
