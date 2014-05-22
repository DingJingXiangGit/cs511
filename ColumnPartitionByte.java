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

public class ColumnPartitionByte{
	public static class Map extends MapReduceBase implements Mapper <Object, Text, Text, Text> {
		private Text key = new Text();
		public void map(Object object, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			key.set(fileSplit.toString());
			context.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, BinaryWritable> {
		private Tuple result;
		private Text outKey = new Text();
		private BinaryWritable outValue = new BinaryWritable();
		private BinaryWritable bytes;
		private String[] headers;
		private int[] types;
		private long[] columnSizes;
		private java.util.Map<String, int[]> columnGroups;
		private java.util.Map<String, ByteArrayOutputStream> columnGroupTable;
		private java.util.Map<String, Long> columnGroupSize;


		private void parseHeaderInfo(String headerString){
			headers = headerString.split(",");
			types = new int[headers.length];
			for(int i = 0; i < headers.length; ++i){
				String[] pairs = headers[i].split(":");
				String type = pairs[1];
				headers[i] = pairs[0];
				if(type.equals("long")){
					types[i] = 0;
				}else if(type.equals("int")){
					types[i] = 1;
				}else if(type.equals("short")){
					types[i] = 2;
				}else if(type.equals("byte")){
					types[i] = 3;
				}else if(type.equals("float")){
					types[i] = 4;
				}else if(type.equals("double")){
					types[i] = 5;
				}else if(type.equals("string")){
					types[i] = 6;
				}
			}
			Tuple.setTypes(this.types);
		}

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
				columnGroupTable = new HashMap<String, ByteArrayOutputStream>();
				columnGroupSize = new HashMap<String, Long>();
				parseHeaderInfo(headerString);
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
					columnGroupTable.put(name, new ByteArrayOutputStream());
					columnGroupSize.put(name, new Long(0));

				}				
				columnSizes = new long[headers.length];


				fis.close();
				reader.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, BinaryWritable> context, Reporter reporter) throws IOException{
			for(int i = 0; i < headers.length; ++i){
				columnSizes[i] = 0;
			}
			for(java.util.Map.Entry<String, Long> group: columnGroupSize.entrySet()){
				columnGroupSize.put(group.getKey(), new Long(0));
			}
		//	StringBuilder builder = new StringBuilder();	
			while(values.hasNext()){
				String item = values.next().toString();
				Tuple tuple = new Tuple(item, ",", 0);
				byte[] token;
				for(int i = 0; i < tuple.length; ++i){
					outKey.set(headers[i]);
					token = tuple.getValue(i);
					columnSizes[i] += token.length;
					outValue.set(token);
					context.collect(outKey, outValue);
				}
		//		builder.append(tuple.properties[0]).append(", ");
		//		builder.append("\n");
				for(java.util.Map.Entry<String, int[]> group: columnGroups.entrySet()){
					int[] groupHeaders = group.getValue();
					String name = group.getKey();
					for(int j = 0; j < groupHeaders.length; ++j){
						token = tuple.getValue(groupHeaders[j]);
						columnGroupTable.get(name).write(token, 0, token.length);
						if(name.equals("region_group")){
							if(j == 0&&token.length!=8 || j >= 1 &&j <=3 &&token.length!=1 || j == 4 &&token.length!=4){
								throw new IOException("j = "+j+"  token.length = "+token.length);
							}
						}
					}
				}
				for(java.util.Map.Entry<String, ByteArrayOutputStream> group: columnGroupTable.entrySet()){
					String name = group.getKey();
					ByteArrayOutputStream baos = group.getValue();
					byte[] value = baos.toByteArray();
					Long size = columnGroupSize.get(name) + value.length;
					outKey.set(name);
					outValue.set(value);

					context.collect(outKey, outValue);
					columnGroupSize.put(name, size);
					baos.reset();
				}

			}
			if(columnGroupSize.get("region_group") % 15 != 0){
				throw new IOException("region_group size error:"+columnGroupSize.get("region_group"));
			}
			for(int i = 0; i < headers.length; ++i){
				outKey.set(headers[i]);
				outValue.set(ByteBuffer.allocate(8).putLong(columnSizes[i]).array());
				context.collect(outKey, outValue);
			}

			for(java.util.Map.Entry<String, Long> group: columnGroupSize.entrySet()){
				outKey.set(group.getKey());
				outValue.set(ByteBuffer.allocate(8).putLong(group.getValue()).array());
				context.collect(outKey, outValue);
			}
		}
final String HEXES = "0123456789ABCDEF";
public String getHex( byte [] raw ) {
    if ( raw == null ) {
        return null;
    }
    final StringBuilder hex = new StringBuilder( 2 * raw.length );
    for ( final byte b : raw ) {
        hex.append(HEXES.charAt((b & 0xF0) >> 4))
            .append(HEXES.charAt((b & 0x0F)));
    }
    return hex.toString();
}
	}



	public static void main (String[] args) throws Exception{
		if(args.length != 3){
			System.err.println("Usage: ColumnPartition <input file> <schema> <column header file>  <output file>");
			System.exit(2);
		}
		JobConf conf = new JobConf(ColumnPartitionByte.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BinaryWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setJarByClass(ColumnPartitionByte.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(ColumnOutputFormatByte.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		conf.set("headers", args[1]);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		JobClient.runJob(conf);
	}
}
