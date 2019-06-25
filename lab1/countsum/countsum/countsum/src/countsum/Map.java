package countsum;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class Map
	  extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
	  private final IntWritable one = new IntWritable(1);
	  private Text word = new Text();
	  
	  public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		  String line = value.toString();
		  StringTokenizer loop = new StringTokenizer(line);
		  while (loop.hasMoreTokens()) {
		    word.set(loop.nextToken().toLowerCase());
			 context.write(word, one);
			
		  }
		}
	}

