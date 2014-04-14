import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.lib.output.*;
public class ColumnPartition{
	public static class Map extends MapReduceBase implements Mapper <Object, Text, Text, Text> {
		private String splitId;
		private Text key = new Text();
		public void setup(Context context) {
			org.apache.hadoop.mapreduce.InputSplit is = context.getInputSplit();
			splitId = MD5Hash.digest(is.toString()).toString();
			key.set(splitId);
		}

		public void map(Object object, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
			context.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, BinaryWritable> {
		private Tuple result;
		private Text outKey = new Text();
		private ColumnSerializer columnSerializer;
		private BinaryWritable bytes;
		private java.util.Map<String, byte[]> columns;
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, BinaryWritable> context, Reporter reporter){
			columnSerializer = new ColumnSerializer(",");
			while(values.hasNext()){
				columnSerializer.addItem(values.next().toString());
			}
			try{
				columns = columnSerializer.getColumns();
				for(java.util.Map.Entry<String, byte[]> entry: columns.entrySet()){	
					outKey.set(entry.getKey());
					bytes = new BinaryWritable();
					bytes.set(entry.getValue());
					context.collect(outKey, bytes);
				}
			}catch(Exception e){
			}
		}
	}

	public static class ColumnOutputFormat extends MultipleSequenceFileOutputFormat<Text,BinaryWritable>{
		protected String generateFileNameForKeyValue(Text key, BinaryWritable value,String name) {
			return key.toString();	
		}
	}

	public static void main (String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: campaignStat <input file> <output file>");
			System.exit(2);
		}
		JobConf conf = new JobConf(ColumnPartition.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BinaryWritable.class);
		conf.setJarByClass(ColumnPartition.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(ColumnOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
