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
	public static class Map extends MapReduceBase implements Mapper <Object, Text, Text, Text> {
		private Text key = new Text();
		public void map(Object object, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			key.set(fileSplit.toString());
			context.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Tuple result;
		private Text outKey = new Text();
		private BinaryWritable bytes;
		private String[] headers;

		public void configure(JobConf job){
			try{
				String headerFile = job.get("headers");
				Path file = new Path(headerFile);//FileOutputFormat.getTaskOutputPath(job, headerFile);
				FileSystem fs = file.getFileSystem(job);
				FSDataInputStream fis = fs.open(file);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
				String headerString = reader.readLine();
				reader.close();
				fis.close();
				headers = headerString.split(",");
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context, Reporter reporter){
			try{
				while(values.hasNext()){
					String item = values.next().toString();
					String[] cols = item.split(",");
					for(int i = 0; i < cols.length; ++i){
						if(headers == null || headers.length <= i){
							outKey.set(i+"");
						}else{
							outKey.set(headers[i]);
						}
						context.collect(outKey, new Text(cols[i].trim()));
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}



	public static void main (String[] args) throws Exception{
		if(args.length != 3){
			System.err.println("Usage: ColumnPartition <input file> <column header file>  <output file>");
			System.exit(2);
		}
		JobConf conf = new JobConf(ColumnPartition.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setJarByClass(ColumnPartition.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInputFormat(ColumnInputFormat.class);
		conf.set("headers", args[1]);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		JobClient.runJob(conf);
	}
}
