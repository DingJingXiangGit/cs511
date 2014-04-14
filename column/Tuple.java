import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class Tuple implements java.io.Serializable, Comparable<Tuple>{
	public long ymdh;
	public String uid;
	public boolean impressions;
	public boolean clicks;
	public boolean conversions;
	public int campaign_id;
	public int creative_id;
	public short region_id;
	public short msa_id;
	public short size_id;
	public double amt_paid_to_media_seller;
	public float amt_paid_to_data_seller;
	public float data_revenue_from_buyer;
	public double media_revenue_from_buyer;
	public double amt_paid_to_broker;
	public int section_id;
	public int site_id;
	public byte netspeed_id;
	public byte user_agent;
	public String tail;

	public Tuple(String entry, String separator){
		String[] tokens = entry.split(separator);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try{
			ymdh = dateFormat.parse(tokens[0]).getTime();
		}catch(Exception e){
			ymdh = 0;
		}
		uid = tokens[1];
		impressions = Boolean.parseBoolean(tokens[2]);
		clicks = Boolean.parseBoolean(tokens[3]);
		conversions = Boolean.parseBoolean(tokens[4]);
		campaign_id = Integer.parseInt(tokens[5]);
		creative_id = Integer.parseInt(tokens[6]);
		region_id = Short.parseShort(tokens[7]);
		msa_id = Short.parseShort(tokens[8]);
		size_id = Short.parseShort(tokens[9]);
		amt_paid_to_media_seller = Double.parseDouble(tokens[10]);
		amt_paid_to_data_seller = Float.parseFloat(tokens[11]);
		data_revenue_from_buyer = Float.parseFloat(tokens[12]);
		media_revenue_from_buyer = Double.parseDouble(tokens[13]);
		amt_paid_to_broker = Double.parseDouble(tokens[14]);
		section_id = Integer.parseInt(tokens[15]);
		site_id = Integer.parseInt(tokens[16]);
		netspeed_id = Byte.parseByte(tokens[17]);
		user_agent = Byte.parseByte(tokens[18]);
		tail = getTail(entry, separator, 19);
	}

	public String getTail(String entry, String separator, int anchor){
		int index = 0;
		for(int i = 0; i < anchor; ++i){
			index = entry.indexOf(separator, index + 1);
		}
		return entry.substring(index+1);
	}
	public String toString(){
		return uid+", "+impressions+", "+clicks+", "+conversions +" tail: "+tail;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(ymdh);
		out.writeChars(uid);
		out.writeBoolean(impressions);
		out.writeBoolean(clicks);
		out.writeBoolean(conversions);
		out.writeInt(campaign_id);
		out.writeInt(creative_id);
		out.writeShort(region_id);
		out.writeShort(msa_id);
		out.writeShort(size_id);
		out.writeDouble(amt_paid_to_media_seller);
		out.writeFloat(amt_paid_to_data_seller);
		out.writeFloat(data_revenue_from_buyer);
		out.writeDouble(media_revenue_from_buyer);
		out.writeDouble(amt_paid_to_broker);
		out.writeInt(section_id);
		out.writeInt(site_id);
		out.writeByte(netspeed_id);
		out.writeByte(user_agent);
		out.writeChars(tail);
	}

	public void readFields(DataInput in) throws IOException {
		ymdh = in.readLong();
		uid = in.readLine();
		impressions = in.readBoolean();
		clicks = in.readBoolean();
		conversions = in.readBoolean();
		campaign_id = in.readInt();
		creative_id = in.readInt();
		region_id = in.readShort();
		msa_id = in.readShort();
		size_id = in.readShort();
		amt_paid_to_media_seller = in.readDouble();
		amt_paid_to_data_seller = in.readFloat();
		data_revenue_from_buyer = in.readFloat();
		media_revenue_from_buyer = in.readDouble();
		amt_paid_to_broker = in.readDouble();
		section_id = in.readInt();
		site_id = in.readInt();
		netspeed_id = in.readByte();
		user_agent = in.readByte();
		tail = in.readLine();
	}

	public int compareTo(Tuple other){
		if(ymdh != other.ymdh){
			Long l1 = new Long(ymdh);
			Long l2 = new Long(other.ymdh);
			return l1.compareTo(l2);
		}
		if(uid.equals(other.uid) == false){
			return uid.compareTo(other.uid);
		}
		if(campaign_id != other.campaign_id){
			return campaign_id - other.campaign_id;
		}
		if(creative_id != other.creative_id){
			return creative_id - other.creative_id;
		}
		return 0;
	}

	
/*
	public static void main(String[] args){
		Tuple tuple = new Tuple("2013-11-18 17:04:23,4093134898702894446,1,0,0,46748392,105881307,908,0,10,0.00611401467065933,0.0,0.0,0.02291458762348069,0.0,7901189,23151252,2,20,B=10&H=http%3A%2F%2Fl.yimg.com%2Frq%2Fdarla,aaaaabbbbbbbbbb", ",");
		System.out.println(tuple);
	}
*/
}
