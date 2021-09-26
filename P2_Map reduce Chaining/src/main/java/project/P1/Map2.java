package project.P1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {

	LongWritable avg = new LongWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] tokens = line.split("\t");

		Text prd = new Text(tokens[0]);

		long rating_avg = Long.parseLong(tokens[2]);

		avg.set(rating_avg);

		context.write(avg, prd);

	}

}
