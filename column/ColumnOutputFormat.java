import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;

public class ColumnOutputFormat extends MultipleTextOutputFormat<Text,Text>{
	protected String generateFileNameForKeyValue(Text key, Text value,String name) {
		return "column-"+key.toString();	
	}

	public RecordWriter<Text, Text> getRecordWriter(FileSystem fs_dummy, JobConf job, String name, Progressable prog) throws IOException{
		return new ColumnRecordWriter<Text, Text>(job, name, prog);
	}


	public class ColumnRecordWriter<K, V> implements RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		private Text delimiter;
		private JobConf job;
		private String name;
		private Progressable prog;

		private java.util.Map<String, DataOutputStream> outputStreams;
		public ColumnRecordWriter(JobConf job, String name, Progressable prog) throws IOException {
			this.job = job;
			this.name = name;
			this.prog = prog;
			this.outputStreams = new Hashtable<String, DataOutputStream>();
			delimiter = new Text("\n");
		}

		private void writeObject(String key, Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				outputStreams.get(key).write(to.getBytes(), 0, to.getLength());
			} else {
				outputStreams.get(key).write(o.toString().getBytes(utf8));
			}
		}

		private void createOutputStream(String key) throws IOException{
			Path file = FileOutputFormat.getTaskOutputPath(job, key);
        	        FileSystem fs = file.getFileSystem(job);
	                FSDataOutputStream fileOut = fs.create(file, prog);
			this.outputStreams.put(key, fileOut);
		}

		public synchronized void write(K key, V value) throws IOException {
			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			
			if (nullKey && nullValue) {
				return;
			}
			String strKey = key.toString();
			if(outputStreams.containsKey(strKey) == false){
				createOutputStream(strKey);
			}
			writeObject(strKey, value);
			writeObject(strKey, delimiter);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			for(java.util.Map.Entry<String, DataOutputStream> entry : outputStreams.entrySet()){
				entry.getValue().close();
			}
		}
	}
}
