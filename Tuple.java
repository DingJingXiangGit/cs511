import java.io.*;
import java.sql.*;
import java.util.*;
import java.nio.*;
import java.lang.*;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple implements java.io.Serializable, Comparable<Tuple>{
	private static int[] types;
	public static final int  LONG = 0;
	public static final int  INT = 1;
	public static final int  SHORT = 2;
	public static final int  BYTE = 3;
	public static final int  FLOAT = 4;
	public static final int  DOUBLE = 5;
	public static final int  STRING = 6;
	private int compIndex;

	public Object[] properties;
	public int length = 0;

	public static void setTypes(int[] ts){
		types = ts;
	}

	public Tuple(String entry, String delimiter, int compareIndex) throws IOException{
		String[] tokens = entry.split(delimiter);
		this.compIndex = compareIndex;
		this.properties = new Object[types.length];
		this.length = types.length;

		for(int i = 0; i < types.length; ++i){
			try{
				switch(types[i]){
					case LONG:
						properties[i] = Long.parseLong(tokens[i].trim());
						break;
					case INT:
						properties[i] = Integer.parseInt(tokens[i].trim());
						break;
					case SHORT:
						properties[i] = Short.parseShort(tokens[i].trim());
						break;
					case BYTE:
						properties[i] = Byte.parseByte(tokens[i].trim());
						break;
					case FLOAT:
						properties[i] = Float.parseFloat(tokens[i].trim());
						break;
					case DOUBLE:
						properties[i] = Double.parseDouble(tokens[i].trim());
						break;
					case STRING:
						properties[i] = tokens[i].trim()+"\n";
						break;

				}
			}catch(Exception e){
				if(types[i] <=3){
					properties[i] = -1;
				}else if(types[i] == 4 ||types[i] == 5){
					properties[i] = 0.0;
				}else{
					properties[i] = "\n";
				}
			}
		}
	}

	public byte[] getValue(int i){
		ByteBuffer buffer;
		try{
		switch(types[i]){
			case LONG:
				buffer = ByteBuffer.allocate(8);
				buffer.putLong(((Long)properties[i]).longValue());
				return buffer.array();
			case INT:
				buffer = ByteBuffer.allocate(4);
				buffer.putInt(((Integer)properties[i]).intValue());
				return buffer.array();
			case SHORT:
				buffer = ByteBuffer.allocate(2);
				buffer.putShort(((Short)properties[i]).shortValue());
				return buffer.array();
			case BYTE:
				buffer = ByteBuffer.allocate(1);
				buffer.put(((Byte)properties[i]).byteValue());
				return buffer.array();
			case FLOAT:
				buffer = ByteBuffer.allocate(4);
				buffer.putFloat(((Float)properties[i]).floatValue());
				return buffer.array();
			case DOUBLE:
				buffer = ByteBuffer.allocate(8);
				buffer.putDouble(((Double)properties[i]).doubleValue());
				return buffer.array();
			case STRING:
				return ((String)properties[i]).getBytes();

		}
		}catch(Exception e){
			if(types[i] == LONG){
				return new byte[8];
			}
			if(types[i] == INT){
				return new byte[4];
			}
			if(types[i] == SHORT){
				return new byte[2];
			}
			if(types[i] == BYTE){
				return new byte[1];
			}
			if(types[i] == FLOAT){
				return new byte[4];
			}
			if(types[i] == DOUBLE){
				return new byte[8];
			}
		}
		return new byte[1];
	}

public int compareTo(Tuple other){
	switch(types[this.compIndex]){
		case LONG:{
				  Long thisValue = (Long)this.properties[this.compIndex];
				  Long otherValue = (Long)other.properties[this.compIndex];
				  return thisValue.compareTo(otherValue);
			  }
		case INT:{
				 Integer thisValue = (Integer)this.properties[this.compIndex];
				 Integer otherValue = (Integer)other.properties[this.compIndex];
				 return thisValue.compareTo(otherValue);
			 }
		case SHORT:{
				   Short thisValue = (Short)this.properties[this.compIndex];
				   Short otherValue = (Short)other.properties[this.compIndex];
				   return thisValue.compareTo(otherValue);
			   }
		case BYTE:{
				  Byte thisValue = (Byte)this.properties[this.compIndex];
				  Byte otherValue = (Byte)other.properties[this.compIndex];
				  return thisValue.compareTo(otherValue);
			  }
		case FLOAT:{
				   Float thisValue = (Float)this.properties[this.compIndex];
				   Float otherValue = (Float)other.properties[this.compIndex];
				   return thisValue.compareTo(otherValue);
			   }
		case DOUBLE:{
				    Double thisValue = (Double)this.properties[this.compIndex];
				    Double otherValue = (Double)other.properties[this.compIndex];
				    return thisValue.compareTo(otherValue);
			    }
		case STRING:{
				    String thisValue = (String)this.properties[this.compIndex];
				    String otherValue = (String)other.properties[this.compIndex];
				    return thisValue.compareTo(otherValue);
			    }
	}
	return 0;
}

public static void main(String[] args){
	int[] types = {2, 2, 6};
	Tuple.setTypes(types);
	try{
		Scanner scanner = new Scanner(new File("sample"));
		parseHeaderInfo("ymdh:string,user_id:long,impressions:char,clicks:char,conversions:char,campaign_id:int,creative_id:int,region_id:int,msa_id:int,size_id:char,amt_paid_to_media_seller:float,amt_paid_to_data_seller:float,data_revenue_from_buyer:float,media_revenue_from_buyer:float,amt_paid_to_broker:float,section_id:int,site_id:int,netspeed_id:byte,user_agent:byte,query_string:string,pop_type_id:byte,roi_cost:string,gender:byte,age:byte,creative_frequency:int,vurl_frequency:int,language_ids:string,isp_id:byte,offer_type_id:string,conversion_id:int,trigger_by_click:byte,ecpm:string,second_ecpm:string,ltv_value:string,rtb_transaction_type:string,first_initial_bid_amount:float,first_initial_bid_price_type:string,second_initial_bid_amount:float,second_initial_bid_price_type:string,screen_type:byte,rich_media_flag:byte,geo_best:string,geo_best_level:string,content_rev_share:string,mobile_wifi:byte,marketplace_type:string,psa_flag:string,house_ad_flag:string,passback_flag:byte,is_coppa:byte,device_segment:int,connection_segment:string,os_segment:int,browser_segment:int,bill_revenue_from_buyer:float,container_type:byte");
		while(scanner.hasNext()){
			String line = scanner.nextLine();
			Tuple tuple = new Tuple(line, ",", 0);
			for(int i = 0; i < tuple.length; ++i){
				System.out.print(tuple.properties[i] +",  ");
			}
			System.out.println();
		}
		scanner.close();
	}catch(Exception e){
		e.printStackTrace();
	}
}
public static void parseHeaderInfo(String headerString){
	String[] headers;
	headers = headerString.split(",");
	int[] types = new int[headers.length];
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
		System.out.println(type +":"+ types[i]);
	}
	Tuple.setTypes(types);
}
}
