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
		private BinaryWritable bytes;
		private String[] headers;
		private int[] columnSizes;

		public void configure(JobConf job){
			try{
				String headerFile = job.get("headers");
				Path file = new Path(headerFile);
				FileSystem fs = file.getFileSystem(job);
				FSDataInputStream fis = fs.open(file);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
				String headerString = reader.readLine();
				reader.close();
				fis.close();
				headers = headerString.split(",");
				columnSizes = new int[headers.length];
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context, Reporter reporter) throws IOException{
			for(int i = 0; i < headers.length; ++i){
				columnSizes[i] = 0;
			}
			while(values.hasNext()){
				String item = values.next().toString();
				String[] cols = item.split(",");
				String token;
				for(int i = 0; i < cols.length; ++i){
					if(headers == null || i >= headers.length){
						continue;
						//outKey.set(i+"");
					}else{
						outKey.set(headers[i]);
					}
					token = cols[i].trim();
					columnSizes[i] += token.length()+1;
					context.collect(outKey, new Text(token));
				}
			}
			for(int i = 0; i < headers.length; ++i){
				outKey.set(headers[i]);
				context.collect(outKey, new Text(String.format("%016d", columnSizes[i])));
			}
		}
		private byte[] getBytes(Object obj) throws Exception{
			if(obj instanceof Double){
				return getBytes((Double)obj);
			}else if(obj instanceof Long){
				return getBytes((Long)obj);
			}else if(obj instanceof Float){
				return getBytes((Float)obj);
			}else if(obj instanceof Integer){
				return getBytes((Integer)obj);
			}else if(obj instanceof Short){
				return getBytes((Short)obj);
			}else if(obj instanceof Byte){
				return getBytes((Byte)obj);
			}else if(obj instanceof Character){
				return getBytes((Character)obj);
			}else{
				return getBytes((String)obj);
			}
		}	
		private byte[] getBytes(double h){
			ByteBuffer bbuf = ByteBuffer.allocate(8);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.putDouble(h);
			return bbuf.array();
		}
		private byte[] getBytes(long h){
			ByteBuffer bbuf = ByteBuffer.allocate(8);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.putLong(h);
			return bbuf.array();
		}
		private byte[] getBytes(float h){
			ByteBuffer bbuf = ByteBuffer.allocate(4);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.putFloat(h);
			return bbuf.array();
		}
		private byte[] getBytes(int h){
			ByteBuffer bbuf = ByteBuffer.allocate(4);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.putInt(h);
			return bbuf.array();
		}
		private byte[] getBytes(short h){
			ByteBuffer bbuf = ByteBuffer.allocate(2);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.putShort(h);
			return bbuf.array();
		}
		private byte[] getBytes(char h){
			ByteBuffer bbuf = ByteBuffer.allocate(1);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.put((byte)h);
			return bbuf.array();
		}
		private byte[] getBytes(byte h){
			ByteBuffer bbuf = ByteBuffer.allocate(1);
			bbuf.order(ByteOrder.BIG_ENDIAN);
			bbuf.clear();
			bbuf.put(h);
			return bbuf.array();
		}
		private byte[] getBytes(String h)throws Exception{
			return h.getBytes("UTF");
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
