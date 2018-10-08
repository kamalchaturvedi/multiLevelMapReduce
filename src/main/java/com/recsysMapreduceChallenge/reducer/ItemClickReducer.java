package com.recsysMapreduceChallenge.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemClickReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	private final static LongWritable finalCount = new LongWritable(1);

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) {
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		finalCount.set(sum);
		try {
			context.write(key, finalCount);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
