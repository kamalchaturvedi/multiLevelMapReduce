package com.recsysMapreduceChallenge.mapper;

import java.io.IOException;
import java.time.ZonedDateTime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemClickMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private final static LongWritable itemCount = new LongWritable(1);
	private Text currentItemId = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String[] splitLine = value.toString().split(",");
		ZonedDateTime date = ZonedDateTime.parse(splitLine[1]);
		if ((splitLine.length > 2) && (date.getMonthValue() == 4)) {
			try {
				currentItemId.set(splitLine[2]);
				itemCount.set(1);
				context.write(currentItemId, itemCount);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
