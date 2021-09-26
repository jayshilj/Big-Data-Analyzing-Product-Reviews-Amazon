package project.P1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		IntWritable c = new IntWritable();
		int count = 0;
		
		

		for (IntWritable val : values) {
			count+= val.get();
		}
		
		c.set(count);
		context.write(key ,c);
	}
}
