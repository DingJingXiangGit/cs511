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

public class ColumnOutputFormatByte extends MultipleSequenceFileOutputFormat<Text, BinaryWritable>{
	protected String generateFileNameForKeyValue(Text key, BinaryWritable value,String name) {
		return "column-"+key.toString();	
	}

	public RecordWriter<Text, BinaryWritable> getRecordWriter(FileSystem fs_dummy, JobConf job, String name, Progressable prog) throws IOException{
		return new ColumnRecordWriter<Text, BinaryWritable>(job, name, prog);
	}


	public class ColumnRecordWriter<K, V> implements RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		private JobConf job;
		private String name;
		private Progressable prog;

		private java.util.Map<String, DataOutputStream> outputStreams;
		public ColumnRecordWriter(JobConf job, String name, Progressable prog) throws IOException {
			this.job = job;
			this.name = name;
			this.prog = prog;
			this.outputStreams = new Hashtable<String, DataOutputStream>();
		}

		private void writeObject(String key, Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				outputStreams.get(key).write(to.getBytes(), 0, to.getLength());
			} else if(o instanceof BinaryWritable){
				BinaryWritable bw = (BinaryWritable) o;	
				bw.write(outputStreams.get(key));
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
		}

		public synchronized void close(Reporter reporter) throws IOException {
			for(java.util.Map.Entry<String, DataOutputStream> entry : outputStreams.entrySet()){
				entry.getValue().close();
			}
		}
	}
}
