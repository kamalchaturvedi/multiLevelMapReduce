package com.recsysMapreduceChallenge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.recsysMapreduceChallenge.mapper.ItemClickMapper;
import com.recsysMapreduceChallenge.mapper.TrimOutputMapper;
import com.recsysMapreduceChallenge.reducer.ItemClickReducer;
import com.recsysMapreduceChallenge.reducer.TrimOutputCombiner;
import com.recsysMapreduceChallenge.reducer.TrimOutputReducer;

public class App {
	static String intermittentOutputPath = "/output-int";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = runItemCount(conf, args);
		boolean jobStatus = job.waitForCompletion(true);
		if (jobStatus) {
			Job sortAndFilterJob = sortAndFilter(conf, args);
			System.exit(sortAndFilterJob.waitForCompletion(true) ? 0 : 1);
		}
	}

	private static Job sortAndFilter(Configuration conf, String[] args) throws IOException {
		Job job = Job.getInstance(conf, "FilterTop20");
		try {
			job.setJarByClass(App.class);
			job.setMapperClass(TrimOutputMapper.class);
			job.setCombinerClass(TrimOutputCombiner.class);
			job.setReducerClass(TrimOutputReducer.class);
			job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job, new Path(intermittentOutputPath));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return job;
	}

	static Job runItemCount(Configuration conf, String[] args) throws IOException {
		Job job = Job.getInstance(conf, "ItemClicks");
		try {
			job.setJarByClass(App.class);
			job.setMapperClass(ItemClickMapper.class);
			job.setCombinerClass(ItemClickReducer.class);
			job.setReducerClass(ItemClickReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(intermittentOutputPath));
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return job;
	}
}
