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

public class WordCount{
    	 
	 public static void main(String[] args) throws Exception {
		 
		 Configuration conf = new Configuration();

		 @SuppressWarnings("deprecation")
		 Job job = new Job(conf, "word count") ;
		 job.setJarByClass(WordCount.class);
		
		 job.setMapperClass(WordCountMapper.class);
		 job.setReducerClass(WordCountReducer.class);
        
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);     
               
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
                
		 job.waitForCompletion(true);
	 }
	 
	 public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		 private final IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 
			 String line = value.toString();
		     StringTokenizer tokenizer = new StringTokenizer(line);
		      
		     while (tokenizer.hasMoreTokens()){
		    	 word.set(tokenizer.nextToken());
		         context.write(word, one);
		     }// while loop
		 }// map
	 }// mapper
	 
	 public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	      
		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
		    while (values.hasNext())
		    	sum += values.next().get();
		    context.write(key, new IntWritable(sum));
		}// reduce
	}// reducer
}
