import java.util.*;
import java.io.*;
import java.lang.*;
import java.lang.reflect.*;

public class ColumnSerializer{
	private Map<String, byte[]> columns;
	private List<Tuple> tuples;
	private Field[] fields;
	private String separator;
	public ColumnSerializer(String sep){
		separator = sep;
		columns = new HashMap<String, byte[]>();
		tuples = new ArrayList<Tuple>();
		fields = Tuple.class.getDeclaredFields();
		for(Field field : fields){
			columns.put(field.getName(), null);
		}
	}

	public void addItem(String entry){
		tuples.add(new Tuple(entry, separator));
	}

	public Map<String, byte[]> getColumns() throws Exception{
		ByteArrayOutputStream baos;
		DataOutputStream w;
		for(Field field : fields){
			baos = new ByteArrayOutputStream();
			w = new DataOutputStream(baos);
			if(field.getType() == int.class){
				for(Tuple tuple: tuples){
					w.writeInt((Integer)field.get(tuple));
				}
			}else if(field.getType() == byte.class){
				for(Tuple tuple: tuples){
					w.writeByte((Byte)field.get(tuple));
				}
			}else if(field.getType() == long.class){
				for(Tuple tuple: tuples){
					w.writeLong((Long)field.get(tuple));
				}
			}else if(field.getType() == double.class){
				for(Tuple tuple: tuples){
					w.writeDouble((Double)field.get(tuple));
				}
			}else if(field.getType() == float.class){
				for(Tuple tuple: tuples){
					w.writeFloat((Float)field.get(tuple));
				}
			}else if(field.getType() == short.class){
				for(Tuple tuple: tuples){
					w.writeShort((Short)field.get(tuple));
				}
			}else if(field.getType() == boolean.class){
				for(Tuple tuple: tuples){
					w.writeBoolean((Boolean)field.get(tuple));
				}
			}else if(field.getType() == String.class){
				for(Tuple tuple: tuples){
					w.writeUTF((String)field.get(tuple));
				}
			}
			w.flush();
			byte[] bytes = baos.toByteArray();
			columns.put(field.getName(), bytes);
			w.close();
			baos.close();
		}
		return columns;
	}
	
}
