package ids;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MeanMaxTemp {

	public static void Main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "mean max temp");
		job.setJarByClass(MeanMaxTemp.class);
		
		job.setMapperClass(MeanMaxTempMapper.class);
		job.setReducerClass(MeanMaxTempReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	public static class MeanMaxTempMapper 
		extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']"; 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString().toLowerCase().replaceAll(tokens, " ");
			StringTokenizer str = new StringTokenizer(line);
			
			while(str.hasMoreTokens()){
				word.set(str.nextToken());
				context.write(word, one);
			}// while loop
		}// map
	}// mapper
	
	public static class MeanMaxTempReducer
		extends Reducer<Text, IntWritable, Text, FloatWritable>{
		
		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			float mean = 0;
			float count = 0;
			float max = 0;
			
			while(values.hasNext()){
				
				float value = values.next().get();
				
				if(count == 0)
					max = value;
				else{
					if(value > max)
						max = value;
				}
				mean += value;
				count += 1;
			}
			
			context.write(key, new FloatWritable(mean/count));
		}// reduce
	}// reducer
}
