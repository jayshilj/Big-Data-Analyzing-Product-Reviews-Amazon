package project.P1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		// First MapReduce
		JobControl jobControl = new JobControl("jobChain");
		Configuration cnf1 = new Configuration();

		Job job1 = Job.getInstance(cnf1);
		job1.setJarByClass(Driver.class);
		job1.setJobName("MR1");

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

		job1.setMapperClass(Map.class);
		job1.setReducerClass(Reduce.class);
		 job1.setCombinerClass(Reduce.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(CompositeKey.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);


		ControlledJob controlledJob1 = new ControlledJob(cnf1);
		controlledJob1.setJob(job1);
		jobControl.addJob(controlledJob1);

		// Second MapReduce

		Configuration cnf2 = new Configuration();

		Job job2 = Job.getInstance(cnf2);
		job2.setJarByClass(Driver.class);
		job2.setJobName("MR2");

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		 job2.setCombinerClass(Reduce2.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setNumReduceTasks(5);
		
		ControlledJob controlledJob2 = new ControlledJob(cnf2);
		controlledJob2.setJob(job2);

		FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
//
		FileSystem fs = FileSystem.get(cnf1);
		fs.delete(new Path(args[1]), true);
//
//		// make job2 dependent on job1
		controlledJob2.addDependingJob(controlledJob1);
//		// add the job to the job control
		jobControl.addJob(controlledJob2);

		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}
