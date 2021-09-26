package project.P1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, CompositeKey, Text,CompositeKey> {
	

	
	@Override
	public void reduce(Text key, Iterable<CompositeKey> values, Context context)
			throws IOException, InterruptedException {

		String productid = " ";
//		String producttitle = " ";
		String out = " ";
		int count = 0;
		Text proddetails = new Text();
		CompositeKey ck = new CompositeKey();
		for (CompositeKey val : values)
		{
			out = val.getProductid() + val.getProduct_title();
			productid = val.getProductid() + val.getProduct_title();
//			producttitle = val.getProduct_title() + " " + producttitle;
			count+= val.getCount();
		}
		
//		out = out + count;
		ck.setCount(count);
		ck.setProductid(productid);
//		ck.setProduct_title(producttitle);
		proddetails.set(out);
		
		context.write(key ,ck);
	}
}
