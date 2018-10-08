package com.recsysMapreduceChallenge.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrimOutputMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	LongWritable count = new LongWritable();
	Text itemId = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String[] splitLine = value.toString().split("\t");
		count.set(Long.parseLong(splitLine[1]));
		itemId.set(splitLine[0]);
		try {
			context.write(count, itemId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
