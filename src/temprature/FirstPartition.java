package temprature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartition extends Partitioner<KeyPair, Text> {

	@Override
	public int getPartition(KeyPair key, Text value, int num) {
		// TODO Auto-generated method stub
		// 按照年份分区
		return key.getYear() * 127 % num;
	}
	 
	

}
