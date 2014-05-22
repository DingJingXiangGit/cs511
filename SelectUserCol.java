import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SelectUserCol{
	public static class Map extends MapReduceBase implements Mapper <Object, Text, Text, Text> {
		private Text userId = new Text();
		private Text dummy = new Text("");

		public void map(Object key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
			String uid = value.toString();
			userId.set(uid);
			context.collect(userId, dummy);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text result = new Text("");
		public void reduce(Text key, Iterator<Text> values,  OutputCollector<Text, Text>  context, Reporter reporter) throws IOException {
			context.collect(key, result);
		}
	}

	public static void main (String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: SelectUserCol <input file> <output file>");
			System.exit(2);
		}
		JobConf conf = new JobConf(SelectUserCol.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setJarByClass(SelectUserCol.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(ColumnInputFormat.class);
		conf.setNumReduceTasks(40);
		conf.set("columns", "user_id");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
