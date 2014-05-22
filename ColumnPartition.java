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

//import org.apache.hadoop.mapreduce.lib.output.*;
public class ColumnPartition{
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
		private Text outValue = new Text();
		private BinaryWritable bytes;
		private String[] headers;
		private int[] columnSizes;
		private java.util.Map<String, int[]> columnGroups;
		private java.util.Map<String, StringBuffer> columnGroupTable;
		private java.util.Map<String, Integer> columnGroupSize;

		public void configure(JobConf job){
			try{
				String line = null;
				String headerFile;
				Path file;
				FileSystem fs;
				FSDataInputStream fis;
				BufferedReader reader;
				String headerString;


				headerFile = job.get("headers");
				file = new Path(headerFile);
				fs = file.getFileSystem(job);
				fis = fs.open(file);
				reader = new BufferedReader(new InputStreamReader(fis));
				headerString = reader.readLine();
				columnGroups = new HashMap<String, int[]>();
				columnGroupTable = new HashMap<String, StringBuffer>();
				columnGroupSize = new HashMap<String, Integer>();

				headers = headerString.split(",");
				while((line = reader.readLine()) != null){
					String[] items = line.split(":");
					String name = items[0];
					String[] columns = items[1].split(",");
					int[] columnIndex = new int[columns.length];
					for(int i = 0; i < columns.length; ++i){
						for(int j = 0; j < headers.length; ++j){
							if(headers[j].equals(columns[i])){
								columnIndex[i] = j;
							}
						}
					}
					columnGroups.put(name, columnIndex);
					columnGroupTable.put(name, new StringBuffer());
					columnGroupSize.put(name, new Integer(0));
				}				
				columnSizes = new int[headers.length];
				fis.close();
				reader.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context, Reporter reporter) throws IOException{
			for(int i = 0; i < headers.length; ++i){
				columnSizes[i] = 0;
			}
			for(java.util.Map.Entry<String, Integer> group: columnGroupSize.entrySet()){
				columnGroupSize.put(group.getKey(), new Integer(0));
			}

			while(values.hasNext()){
				String item = values.next().toString();
				String[] cols = item.split(",");
				String token;
				System.err.println(item);
				for(int i = 0; i < cols.length; ++i){
					if(headers == null || i >= headers.length){
						continue;
					}else{
						outKey.set(headers[i]);
						token = cols[i].trim();
						columnSizes[i] += token.length() + 1;
						outValue.set(token);
						context.collect(outKey, outValue);
						for(java.util.Map.Entry<String, int[]> group: columnGroups.entrySet()){
							int[] groupHeaders = group.getValue();
							String name = group.getKey();
							for(int j = 0; j < groupHeaders.length; ++j){
								if(i == groupHeaders[j]){
									columnGroupTable.get(name).append(cols[i].trim()).append(",");
								}
							}
						}
					}
				}
				for(java.util.Map.Entry<String, StringBuffer> group: columnGroupTable.entrySet()){
					String name = group.getKey();
					StringBuffer buffer = group.getValue();
					String value = buffer.toString().substring(0, buffer.length() - 1);
					Integer size = columnGroupSize.get(name) + value.length() + 1;
					outKey.set(name);
					outValue.set(value);
					context.collect(outKey, outValue);
					columnGroupSize.put(name, size);
					buffer.delete(0, buffer.length());
				}
				
			}

			for(int i = 0; i < headers.length; ++i){
				outKey.set(headers[i]);
				outValue.set(String.format("%016d", columnSizes[i]));
				context.collect(outKey, outValue);//new Text(String.format("%016d", columnSizes[i])));
			}

			for(java.util.Map.Entry<String, Integer> group: columnGroupSize.entrySet()){
				outKey.set(group.getKey());
				outValue.set(String.format("%016d", group.getValue().intValue()));
				context.collect(outKey, outValue);
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
		conf.setOutputFormat(ColumnOutputFormat.class);
		conf.set("headers", args[1]);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		JobClient.runJob(conf);
	}
}
