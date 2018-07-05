package temprature;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RunJob {
	
	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static class HotMapper extends Mapper<LongWritable, Text, KeyPair, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] ss = line.split("\t");
			if(ss.length == 2) {
				Date date;
				try {
					date = SDF.parse(ss[0]);
					Calendar c = Calendar.getInstance();
					c.setTime(date);
					int year = c.get(1);
					String hot = ss[1].substring(0, ss[1].indexOf("-C"));
					KeyPair kp = new KeyPair();
					kp.setYear(year);
					kp.setHot(Integer.parseInt(hot));
					context.write(kp, value);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
 		}
	}

	static class HotReduce extends Reducer<KeyPair, Text, KeyPair, Text>{

		@Override
		protected void reduce(KeyPair kp, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			 for (Text v: value ) {
				 context.write(kp, v);
			 }
		}
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		 
		try {
			Job job  = new Job(conf);
			job.setJobName("hot");
			job.setJarByClass(RunJob.class);
			job.setMapperClass(HotMapper.class);
			job.setReducerClass(HotReduce.class);
			job.setMapOutputKeyClass(KeyPair.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setNumReduceTasks(3); //设置reduce任务的个数
			job.setPartitionerClass(FirstPartition.class);
			job.setSortComparatorClass(SortHot.class);
			job.setGroupingComparatorClass(GroupHot.class);
			
			//mapreduce 输入数据所在目录或者文件
			FileInputFormat.addInputPath(job, new Path("/usr/input/temp"));
			//mapreduce执行之后的输出数据的目录
			FileOutputFormat.setOutputPath(job, new Path("/usr/output/temp"));
			System.exit(job.waitForCompletion(true)? 0:1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 

	}

}
