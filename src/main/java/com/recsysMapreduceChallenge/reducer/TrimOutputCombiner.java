package com.recsysMapreduceChallenge.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrimOutputCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	Integer counter = 0;
	Integer maximumOutputSize = 20;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		counter = 0;
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) {
		if (counter < maximumOutputSize) {
			for (Text value : values) {
				try {
					context.write(key, value);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				++counter;
				if (counter >= maximumOutputSize)
					break;
			}
		}
	}
}
