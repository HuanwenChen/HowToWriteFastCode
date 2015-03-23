package mapred.hashtagsim;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by ChenHuanWen on 3/18/15.
 */
public class SimilarityReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value:values){
            count += value.get();
        }
        context.write(new IntWritable(count),key);
    }
}
