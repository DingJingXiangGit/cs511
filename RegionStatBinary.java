import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RegionStatBinary{
	public static class Map extends MapReduceBase implements Mapper <Object, Text, Text, Text> {
		private Text regionId = new Text();
		private Text valueStr = new Text();
		public void map(Object key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");
			String impression = tokens[1];
			String click = tokens[2];
			String conversion = tokens[3];
			String campaign = tokens[4];
			String entry = impression + ","+click+","+conversion;
			valueStr.set(entry);
			regionId.set(campaign);
			context.collect(regionId, valueStr);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text result = new Text();	
		public void reduce(Text key, Iterator<Text> values,  OutputCollector<Text, Text>  context, Reporter reporter) throws IOException {
			int impressions = 0;
			int clicks = 0;
			int conversions = 0;
			StringBuffer buffer = new StringBuffer();
			while(values.hasNext()){
				String entry = values.next().toString();
				String[] items = entry.split(",");
				if(items[0].equals("1")){
					impressions += 1;
				}
				if(items[1].equals("1")){
					clicks += 1;
				}
				if(items[2].equals("1")){
					conversions += 1;
				}
			}
			buffer.append(impressions).append(",");
			buffer.append(clicks).append(",");
			buffer.append(conversions);
			result.set(buffer.toString());
			context.collect(key, result);
		}
	}

	public static void main (String[] args) throws Exception{
		if(args.length != 4){
			System.err.println("Usage: campaignStat <input file> <output file>");
			System.exit(2);
		}
		JobConf conf = new JobConf(RegionStatBinary.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(40);
		conf.setInputFormat(ColumnInputFormatByte.class);
		conf.set("columns", args[2]);
		conf.set("schema", args[1]);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[3]));
		JobClient.runJob(conf);
	}
}
