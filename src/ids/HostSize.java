package ids;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class HostSize {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "host size");
		
		job.setMapperClass(HostSizeMapper.class);
		job.setReducerClass(HostSizeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	public static class HostSizeMapper 
				extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			StringTokenizer str = new StringTokenizer(line);
			
			while(str.hasMoreTokens()){
				word.set(str.nextToken());
				context.write(word, one);
			}// while loop	
		}//map 
	}//mapper
	
	public static class HostSizeReducer 
				extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			int size = 0;
			while(values.hasNext())
				size += values.next().get();
				context.write(key, new IntWritable(size));
		}// reduce
	}// reducer
	
}// class
