import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.lib.*;


public class ColumnInputFormat extends CombineFileInputFormat<Text, Text>  {
	public ColumnInputFormat(){
		super();
	}
	private int FOOT_SIZE=17;
	public String[] loadColumns(String columnFile, JobConf job) throws IOException{
		Path file = new Path(columnFile);
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fis = fs.open(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		String headerString = reader.readLine();
		reader.close();
		fis.close();
		return headerString.split(",");
	}
	public InputSplit[] getSplits(JobConf job, int numSplit){
		try{
			String columnFile = job.get("columns");
			String[] columns = columnFile.split(",");//loadColumns(columnFile, job);
			List<List<FileSplit>> splits = new LinkedList<List<FileSplit>>();

			Path file;	
			FileSystem fs;
			FSDataInputStream fis;
			long offset = 0;
			long size = 0;
			FileSplit split;
			List<FileSplit> splitList;
			int splitNum = 0;
			int numHeaders = columns.length;
			byte[] byteBuffer = new byte[16];
			String basePath = this.getInputPaths(job)[0].toString();
			for(int i = 0; i < numHeaders; ++i){
				file = new Path(basePath+"/"+columns[i]);
				splitList = new LinkedList<FileSplit>();
				fs = file.getFileSystem(job);
				fis = fs.open(file);
				offset = fs.getFileStatus(file).getLen();
				while(offset>0){
					//try{
					fis.readFully(offset - FOOT_SIZE, byteBuffer);
					size = Long.parseLong(new String(byteBuffer));
					offset -= (size + FOOT_SIZE);
					split = new FileSplit(file, offset, size, job);
					splitList.add(split);
					//}catch(EOFException e){
				//		throw new EOFException("eof: offset = "+offset +", size = " + fs.getFileStatus(file).getLen() +" footer:"+ new String(byteBuffer));
					//}
				}
				splits.add(splitList);
				splitNum = splitList.size();
				fis.close();
			}

			//List<FileSplit>[] splitLists = (List<FileSplit>[])splits.toArray();
			List<CombineFileSplit> combineFileSplits = new ArrayList<CombineFileSplit>();

			for(int i = 0; i < splitNum; ++i){
				Path[] paths = new Path[numHeaders];
				long[] starts = new long[numHeaders];
				long[] lengths = new long[numHeaders];
				List<String> locations = new ArrayList<String>();
				for(int j = 0; j < numHeaders; ++j){
					paths[j] = splits.get(j).get(i).getPath();
					starts[j] = splits.get(j).get(i).getStart();
					lengths[j] = splits.get(j).get(i).getLength();
					locations.addAll(Arrays.asList(splits.get(j).get(i).getLocations()));
				}
				CombineFileSplit combineSplit = new CombineFileSplit(job, paths, starts, lengths, new String[0]);//(String[])locations.toArray());
				combineFileSplits.add(combineSplit);
			}
			return combineFileSplits.toArray(new CombineFileSplit[0]);
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;
	}

	public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException{
		return new ColumnRecordReader((CombineFileSplit)split, job);
	}

	protected boolean isSplitable(JobContext context, Path file){
		return false;
	}

	public static class ColumnRecordReader implements  RecordReader<Text, Text>{
		private CombineFileSplit split;
		private JobConf job;
		private long start[];
		private long pos[];
		private long end[];
		private Text key;
		private Text value;
		private Scanner[] readers;
		private int columns;
		public ColumnRecordReader(CombineFileSplit split, JobConf job )throws IOException{
			this.split = split;
			this.job = job;
			columns = split.getNumPaths();
			key = new Text();
			value = new Text();
			readers = new Scanner[columns];
			start = split.getStartOffsets();
			pos = new long[columns];
			end = new long[columns];
			initialize();
		}

		private void initialize() throws IOException{
			FSDataInputStream fis;
			FileSystem fs;
			Path file;
			byte[] buffer;
			for(int i = 0; i < readers.length; ++i){
				pos[i] = start[i];
				end[i] = start[i] + split.getLength(i);
				buffer = new byte[(int)split.getLength(i)];
				file = split.getPath(i);
				fs = file.getFileSystem(job);
				fis = fs.open(file);
				fis.seek(start[i]);
				fis.readFully(buffer);
				readers[i] = new Scanner(new String(buffer));
				fis.close();
			}
		}

		public float getProgress(){
			return (pos[0] - start[0])/(end[0] - start[0]);
		}
		public long getPos(){
			return pos[0];
		}
		public Text createValue(){
			return value;
		}
		public Text createKey(){
			return key;
		}
		public void close()throws IOException{
			for(int i = 0; i < readers.length; ++i){
				readers[i].close();
			}
		}
		public boolean next(Text key, Text value) throws IOException{
			if(pos[0] < end[0]){
				StringBuffer buff = new StringBuffer();
				//Text token = new Text();
				String token;
				for(int i = 0; i < readers.length; ++i){
					token = readers[i].nextLine();
					//readers[i].readLine(token);
					buff.append(token).append(",");
					pos[i] += token.length() + 1;
				}
				value.set(buff.toString());
				this.value.set(buff.toString());
				this.key.set(key.toString());
				return true;
			}else{
				return false;
			}
		}
	}
}
