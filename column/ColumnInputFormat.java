import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
	public InputSplit[] getSplits(JobConf job, int numSplit){
		try{
			String columns = job.get("columns");
			String[] selectedFiles = columns.split(",");
			List<List<FileSplit>> splits = new LinkedList<List<FileSplit>>();

			Path file;	
			FileSystem fs;
			FSDataInputStream fis;
			long offset = 0;
			long size = 0;
			FileSplit split;
			List<FileSplit> splitList;
			int splitNum = 0;
			int numHeaders = selectedFiles.length;
			byte[] byteBuffer = new byte[16];
			for(int i = 0; i < selectedFiles.length; ++i){
				file = new Path(selectedFiles[i]);
				splitList = new LinkedList<FileSplit>();
				fs = file.getFileSystem(job);
				fis = fs.open(file);
				offset = fs.getFileStatus(file).getLen();
				while(offset>0){
					fis.seek(offset - FOOT_SIZE);
					fis.readFully(offset, byteBuffer);
					size = Long.parseLong(new String(byteBuffer));
					split = new FileSplit(file, offset, size, job);
					splitList.add(split);
					offset -= size;
				}
				splits.add(splitList);
				splitNum = splitList.size();
			}

			List<FileSplit>[] splitLists = (List<FileSplit>[])splits.toArray();
			List<CombineFileSplit> combineFileSplits = new ArrayList<CombineFileSplit>();

			for(int i = 0; i < splitNum; ++i){
				Path[] paths = new Path[numHeaders];
				long[] starts = new long[numHeaders];
				long[] lengths = new long[numHeaders];
				List<String> locations = new ArrayList<String>();
				for(int j = 0; j < numHeaders; ++j){
					paths[j] = splitLists[j].get(i).getPath();
					starts[j] = splitLists[j].get(i).getStart();
					lengths[j] = splitLists[j].get(i).getLength();
					locations.addAll(Arrays.asList(splitLists[j].get(i).getLocations()));
				}
				CombineFileSplit combineSplit = new CombineFileSplit(job, paths, starts, lengths, (String[])locations.toArray());
				combineFileSplits.add(combineSplit);
			}
			return (CombineFileSplit[])combineFileSplits.toArray();
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;
	}
	/*
	public RecordReader<Text,Text>  createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new ColumnRecordReader<Text, Text>((CombineFileSplit)split, context);
	}
	*/

	public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException{
		return new ColumnRecordReader<Text, Text>((CombineFileSplit)split, job);
	}
	
	protected boolean isSplitable(JobContext context, Path file){
		return false;
	}

	public static class ColumnRecordReader<K, V> implements  RecordReader<K, V>{
		public ColumnRecordReader(CombineFileSplit split, JobConf job ){
		}
		public float getProgress(){
			return 0.1f;	
		}
		public long getPos(){
			return 0;
		}
		public V createValue(){
			return null;
		}
		public K createKey(){
			return null;
		}
		public void close(){
			//
		}
		public boolean next(K key, V value){
			return true;
		}
	}
}

