package friends;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>{
public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException{
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
        String line = null;
        String[] lineArray = null;
        String[] friendArray = null;
        String[] tempArray = null;
        while(tokenizer.hasMoreTokens()){
                line = tokenizer.nextToken();
                lineArray = line.split(" -> ");
                friendArray = lineArray[1].split(" ");
                tempArray = new String[2];
                for(int i = 0; i < friendArray.length; i++){
                        tempArray[0] = friendArray[i];
                        tempArray[1] = lineArray[0];
                        Arrays.sort(tempArray);
                        output.collect(new Text(tempArray[0] + " " + tempArray[1]), new Text(lineArray[1]));
                }
        }
}
}
