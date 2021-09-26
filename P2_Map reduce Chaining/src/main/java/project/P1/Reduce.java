package project.P1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<Text, CompositeKey, Text, CompositeKey> {

	
	CompositeKey ck = new CompositeKey();
	@Override
	protected void reduce(Text key, Iterable<CompositeKey> values, Context context)
			throws IOException, InterruptedException {
		
		long sum = 0;
		long count = 0;
		long avg = 0;

		for (CompositeKey val : values) 
		{
			sum += val.getRating_avg() * val.getCount() ;
			count = count + val.getCount();
		}
		
		avg = sum/count;
		
		ck.setCount(count);
		ck.setRating_avg(avg);

		context.write(key, ck);

	}

}
