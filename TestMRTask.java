import java.io.IOException;
import java.util.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TestMRTask{
	public static class Map extends MapReduceBase implements Mapper <Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
			context.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
		private NullWritable nullKey = NullWritable.get();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> context, Reporter reporter)throws IOException{
			while(values.hasNext()){
				context.collect(nullKey, values.next());
			}
		}
	}



	public static void main (String[] args) throws Exception{
		if(args.length != 3){
			System.err.println("Usage: ColumnPartition <input file> <column header file>  <output file>");
			System.exit(2);
		}
		JobConf conf = new JobConf(TestMRTask.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setJarByClass(TestMRTask.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInputFormat(ColumnInputFormat.class);
		conf.set("columns", args[1]);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		JobClient.runJob(conf);
	}
}
