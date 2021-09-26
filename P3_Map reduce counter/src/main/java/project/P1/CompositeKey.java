package project.P1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements Writable {
	private String productid;
	private String product_title;
	private long count;
	

	public String getProductid() {
		return productid;
	}

	public void setProductid(String productid) {
		this.productid = productid;
	}

	public String getProduct_title() {
		return product_title;
	}

	public void setProduct_title(String product_title) {
		this.product_title = product_title;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public CompositeKey() {
		super();
	}

	

	public CompositeKey(String productid, String product_title, long count) {
		super();
		this.productid = productid;
		this.product_title = product_title;
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
		productid = in.readUTF();
		product_title = in.readUTF();
		count = in.readLong();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(productid);
		out.writeUTF(product_title);
		out.writeLong(count);
		
	}

	@Override
	public String toString() {
		return productid + "\t" + product_title + "\t" + count;
	}

}