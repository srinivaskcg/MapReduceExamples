package ids;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

public class TopNWords {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "top n words");
		
		job.setJarByClass(TopNWords.class);
		
		job.setMapperClass(TopNWordsMapper.class);
		job.setReducerClass(TopNWordsReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
		
	}
	
	public static class TopNWordsMapper 
			extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			String line = value.toString().toLowerCase().replaceAll(tokens, " ");
			StringTokenizer str = new StringTokenizer(line);
			
			while(str.hasMoreTokens()){
				word.set(str.nextToken().trim());
				context.write(word,  one);
			}
		}// map
	}// mapper
	
	public static class TopNWordsReducer 
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private Map<Text, IntWritable> countMap = new HashMap<>();
		private Integer limiter = new Integer(20);

		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			while(values.hasNext())
				sum += values.next().get();
			
			countMap.put(key, new IntWritable(sum));
			
			Map<Text, IntWritable> sortedMap = sortByValue(countMap);
			
			int counter = 0;
			while(sortedMap.containsKey(key)){
				if(counter++ == limiter)
					break;
				context.write(key, sortedMap.get(key));
			}
		}// reduce
	}// reduce
	
	public static <K, V extends Comparable<? super V>> Map<K, V> 
    		sortByValue( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
		{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 ){
				return (o1.getValue()).compareTo( o2.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		
		for (Map.Entry<K, V> entry : list)
			result.put( entry.getKey(), entry.getValue() );
		
		return result;
	}

}
