package countsum;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

private MultipleOutputs<Text, IntWritable> multipleOutputs;

public void setup(Context context)
  throws IOException, InterruptedException {
   multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
}

public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	 int sum=0;
	 for (IntWritable value : values) {
	       sum +=value.get();
	    }
		multipleOutputs.write(key, new IntWritable(sum), key.toString());
}

public void cleanup(Context context)
  throws IOException, InterruptedException {
multipleOutputs.close();

}
}
