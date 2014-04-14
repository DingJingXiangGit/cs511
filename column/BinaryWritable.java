import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
public class BinaryWritable implements Writable{
	private byte[] bytes;
	public void readFields(DataInput input) throws IOException{
		int size = input.readInt();
		byte[] bytes = new byte[size];
		input.readFully(bytes);
	}
	public void write(DataOutput output) throws IOException{
		if(bytes != null){
			output.writeInt(bytes.length);
			output.write(bytes);
		}
	}
	public byte[] get(){
		return bytes;
	}
	public void set(byte[] bytes){
		this.bytes = bytes;
	}
}
