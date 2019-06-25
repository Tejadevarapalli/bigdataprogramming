package countsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 
public class Countsum {
	

	public static void main(String[] args) throws Exception {
		
		  Configuration conf = new Configuration();
		  Job job = new Job(conf, "Countsum");
		  job.setJarByClass(Countsum.class);
		  job.setMapperClass(Map.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  job.setReducerClass(Reduce.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileInputFormat.addInputPath(job, new Path(args[1]));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  
		  job.waitForCompletion(true);
		}
}

