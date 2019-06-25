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


public class Reduce extends MapReduceBase
implements Reducer<Text, Text, Text, Text>{
public void reduce(Text key, Iterator<Text> values,
OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
        Text[] texts = new Text[2];
        int index = 0;
        while(values.hasNext()){
                texts[index++] = new Text(values.next());
        }
        String[] list1 = texts[0].toString().split(" ");
        String[] list2 = texts[1].toString().split(" ");
        List<String> list = new LinkedList<String>();
        for(String friend1 : list1){
                for(String friend2 : list2){
                        if(friend1.equals(friend2)){
                                list.add(friend1);
                        }
                }
        }
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < list.size(); i++){
                sb.append(list.get(i));
                if(i != list.size() - 1)
                        sb.append(" ");
        }
        output.collect(key, new Text(sb.toString()));
}
}