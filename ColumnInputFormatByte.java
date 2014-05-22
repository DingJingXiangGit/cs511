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


public class ColumnInputFormatByte extends CombineFileInputFormat<Text, Text>  {
	public ColumnInputFormatByte(){
		super();
	}
	private int FOOT_SIZE = 8;
	private Map<String, Integer> columnTypes = new HashMap<String, Integer>();
	private Map<String, Integer[]> columnGroupTypes = new HashMap<String, Integer[]>();

	public void loadColumnTypes(String columnFile, JobConf job) throws IOException{
		Path file = new Path(columnFile);
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fis = fs.open(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		String headerString = reader.readLine();
		String[] columns = headerString.split(",");
		for(int i = 0; i < columns.length; ++i){
			String[] pairs = columns[i].split(":");
			String type = pairs[1];
			int value = -1;
			if(type.equals("long")){
				value = 0;
			}else if(type.equals("int")){
				value = 1;
			}else if(type.equals("short")){
				value = 2;
			}else if(type.equals("byte")){
				value = 3;
			}else if(type.equals("float")){
				value = 4;
			}else if(type.equals("double")){
				value = 5;
			}else if(type.equals("string")){
				value = 6;
			}
			columnTypes.put(pairs[0], value);
		}

		String group;
		String groupName;
		Integer[] groupPropertyTypes;
		while((group = reader.readLine()) != null){
			String[] pairs = group.split(":");
			columns = pairs[1].split(",");
			groupName = pairs[0];
			groupPropertyTypes = new Integer[columns.length];
			for(int i = 0; i < columns.length; ++i){
				groupPropertyTypes[i] = columnTypes.get(columns[i]);
			}
			columnGroupTypes.put(groupName, groupPropertyTypes);
		}
		reader.close();
		fis.close();
	}

	public InputSplit[] getSplits(JobConf job, int numSplit){
		try{
			loadColumnTypes(job.get("schema"), job);
			String columnFile = job.get("columns");
			String[] columns = columnFile.split(",");
			List<List<FileSplit>> splits = new LinkedList<List<FileSplit>>();
			List<Integer> typesList = new LinkedList<Integer>();
			Path file;	
			FileSystem fs;
			FSDataInputStream fis;
			long offset = 0;
			long size = 0;
			FileSplit split;
			List<FileSplit> splitList;
			int splitNum = 0;
			int numHeaders = columns.length;
			byte[] byteBuffer = new byte[FOOT_SIZE];
			String basePath = this.getInputPaths(job)[0].toString();
			for(int i = 0; i < numHeaders; ++i){
				file = new Path(basePath+"/"+columns[i]);
				splitList = new LinkedList<FileSplit>();
				fs = file.getFileSystem(job);
				fis = fs.open(file);
				offset = fs.getFileStatus(file).getLen();
				while(offset > 0){
					fis.readFully(offset - FOOT_SIZE, byteBuffer);
					size = ByteBuffer.wrap(byteBuffer).getLong();
					/*
					if(size%15 != 0){
						//throw IOException("size error: "+size);
						System.err.println("size error: "+size);
					}
Q					*/
					offset -= (size + FOOT_SIZE);
					split = new FileSplit(file, offset, size, job);
					splitList.add(split);
				}
				typesList.add(columnTypes.get(columns[i]));
				splits.add(splitList);
				splitNum = splitList.size();
				fis.close();
			}

			List<InputSplit> combineFileSplits = new ArrayList<InputSplit>();
			Integer[] types = typesList.toArray(new Integer[0]);
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
				ColumnsSplit combineSplit = new ColumnsSplit(job, paths, starts, lengths, columns, columnTypes, columnGroupTypes, new String[0] );//types, new String[0]);
				combineFileSplits.add(combineSplit);
			}
			return combineFileSplits.toArray(new ColumnsSplit[0]);
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;
	}

	public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException{
		return new ColumnRecordReader((ColumnsSplit)split, job);
	}

	protected boolean isSplitable(JobContext context, Path file){
		return false;
	}

	public static class ColumnsSplit implements InputSplit {
		private JobConf job;
		private Path[] paths;
		private long[] starts;
		private long[] lengths;
		private String[] locations;
		private String[] columns;
		private Map<String, Integer> columnTypes;
		private Map<String, Integer[]> columnGroupTypes;	
		public ColumnsSplit(){}

		public ColumnsSplit(JobConf job, Path[] paths, long[] starts, long[] lengths, String[] columns, 
				Map<String, Integer> columnTypes, Map<String, Integer[]>columnGroupTypes, String[] locations){
			this.job = job;
			this.paths = paths;
			this.starts = starts;
			this.lengths = lengths;
			this.locations = locations;
			this.columns = columns;
			this.columnTypes = columnTypes;
			this.columnGroupTypes = columnGroupTypes;
		}

		public String[] getColumns(){
			return columns;
		}

		public Map<String, Integer> getColumnTypes(){
			return columnTypes;
		}

		public Map<String, Integer[]> getColumnGroupTypes(){
			return columnGroupTypes;
		}

		public JobConf getJobConf(){
			return job;	
		}

		public Path[] getPaths(){
			return this.paths;
		}

		public long[] getStartOffsets(){
			return this.starts;
		}

		public long[] getLengths(){
			return this.lengths;
		}

		public Path getPath(int i){
			return paths[i];
		}

		public int getNumPaths(){
			return paths.length;
		}

		public long getLength(int i){
			return lengths[i];
		}

		public long getLength(){
			long length = 0;
			for(int i = 0; i < lengths.length; ++i){
				length += lengths[i];
			}
			return length;
		}

		public String[] getLocations(){
			return locations;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(paths.length);
			for(int i = 0; i < paths.length; ++i){
				Text.writeString(out, paths[i].toString());
				out.writeLong(starts[i]);
				out.writeLong(lengths[i]);
				Text.writeString(out, columns[i]);
			}
			out.writeInt(columnTypes.size());
			for(java.util.Map.Entry<String, Integer> type : columnTypes.entrySet()){
				Text.writeString(out, type.getKey());	
				out.writeInt(type.getValue().intValue());
			}

			out.writeInt(columnGroupTypes.size());
			for(java.util.Map.Entry<String, Integer[]> group : columnGroupTypes.entrySet()){
				Integer[] types = group.getValue();
				Text.writeString(out, group.getKey());	
				out.writeInt(types.length);
				for(int i = 0; i < types.length; ++i){
					out.writeInt(types[i]);
				}
			}
		}

		public void readFields(DataInput in) throws IOException{
			int size = in.readInt();
			int numTypes = 0;
			int numGroups = 0;
			paths = new Path[size];
			starts = new long[size];
			lengths = new long[size];
			columns = new String[size];
			columnTypes = new HashMap<String, Integer>();
			columnGroupTypes = new HashMap<String, Integer[]>();
			for(int i = 0; i < size; ++i){
				paths[i] = new Path(Text.readString(in));
				starts[i] = in.readLong();
				lengths[i] = in.readLong();
				columns[i] = Text.readString(in);
			}
			numTypes = in.readInt();
			for(int i = 0; i < numTypes; ++i){
				columnTypes.put(Text.readString(in), in.readInt());
			}

			numGroups = in.readInt();
			for(int i = 0; i < numGroups; ++i){
				String name = Text.readString(in);
				int s = in.readInt();
				Integer[] entry = new Integer[s];
				for(int j = 0; j < s; ++j){
					entry[j] = in.readInt();
				}
				columnGroupTypes.put(name, entry);
			}
		}
	}
	public static class ColumnRecordReader implements  RecordReader<Text, Text>{
		private ColumnsSplit split;
		private JobConf job;
		private long[] start;
		private long[] pos;
		private long[] end;
		private int[] types;
		private Text key;
		private Text value;
		private DataInputStream[] readers;
		private Map<String, Integer> typeMap;
		private Map<String, Integer[]> groupMap;
		private String[] columns;


		public ColumnRecordReader(ColumnsSplit split, JobConf job )throws IOException{
			this.split = split;
			this.job = job;
			int numColumns = split.getNumPaths();
			key = new Text();
			value = new Text();
			readers = new DataInputStream[numColumns];
			start = split.getStartOffsets();
			pos = new long[numColumns];
			end = new long[numColumns];
			columns = split.getColumns();
			typeMap = split.getColumnTypes();
			groupMap = split.getColumnGroupTypes();
			//types = split.getTypes();
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
				readers[i] = new DataInputStream(new ByteArrayInputStream(buffer));
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
			//try{
				//if(pos[0] < end[0]){
				long available = readers[0].available();
				if(available != 0){
					//System.err.println("current position = "+pos[0]+"   end position = "+end[0]);
					StringBuffer buff = new StringBuffer();
					String token = "";
					for(int i = 0; i < readers.length; ++i){
						if(typeMap.containsKey(columns[i])){
							int type = typeMap.get(columns[i]);
							if(type == 0){
								token = Long.toString(readers[i].readLong());
								pos[i] += 8;
							}else if(type == 1){
								token = Integer.toString(readers[i].readInt());
								pos[i] += 4;
							}else if(type == 2){
								token = Short.toString(readers[i].readShort());
								pos[i] += 2;
							}else if(type == 3){
								token = Byte.toString(readers[i].readByte());
								pos[i] += 1;
							}else if(type == 4){
								token = Float.toString(readers[i].readFloat());
								pos[i] += 4;
							}else if(type == 5){
								token = Double.toString(readers[i].readDouble());
								pos[i] += 8;
							}else if(type == 6){
								token = readers[i].readLine();
								pos[i] += token.length() + 1;
							}
						}else{
							Integer[]  types = groupMap.get(columns[i]);
							StringBuffer sb = new StringBuffer();
							for(int j = 0; j < types.length; ++j){
								//try{
								if(types[j] == 0){
									token = Long.toString(readers[i].readLong());
									pos[i] += 8;
								}else if(types[j] == 1){
									token = Integer.toString(readers[i].readInt());
									pos[i] += 4;
								}else if(types[j] == 2){
									token = Short.toString(readers[i].readShort());
									pos[i] += 2;
								}else if(types[j] == 3){
									token = Byte.toString(readers[i].readByte());
									pos[i] += 1;
								}else if(types[j] == 4){
									token = Float.toString(readers[i].readFloat());
									pos[i] += 4;
								}else if(types[j] == 5){
									token = Double.toString(readers[i].readDouble());
									pos[i] += 8;
								}else if(types[j] == 6){
									token = readers[i].readLine();
									pos[i] += token.length() + 1;
								}
								if(j != types.length - 1){
									sb.append(token).append(",");
								}else{
									sb.append(token);
								}/*
								}catch(Exception e){
									throw new IOException("j = "+j+" i = "+i +" pos = "+pos[i]+"  start = " +start[i] +" end = "+end[i] + " rest = "+readers[i].available() + " available = "+available, e);
								}*/
							}
							token = sb.toString();
						}
						buff.append(token).append(",");
					}
					value.set(buff.toString());
					this.value.set(buff.toString());
					this.key.set(key.toString());
					return true;
				}else{
					return false;
				}
			//}catch(Exception e){
				/*
				StringBuilder builder = new StringBuilder();
				builder.append("total = "+pos.length);
				for(int i = 0; i < pos.length;++i){
					builder.append(String.format("pos[%d] = %d, end[%d] = %d", i, pos[i], i, end[i]));
					builder.append(readers[i].available());
				}
				throw new IOException(builder.toString());
				*/
				//return false;
			//}
			}
		}
	}
