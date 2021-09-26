package project.P1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<LongWritable, Text, LongWritable, Text> {

	LongWritable avg = new LongWritable();

	protected void reduce(LongWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		int counter = 0;
		for (Text val : value) {
		counter++;
		if(counter<= 10)
		{
				context.write(key, val);
			}

		}

	}
}
