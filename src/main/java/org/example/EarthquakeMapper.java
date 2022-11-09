package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class EarthquakeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",", 12);
        if (line.length != 12) {
            System.out.println("- " + line.length);
            return;
        }
        String outputKey = line[11];
        double outputValue = Double.parseDouble(line[8]);
        context.write(new Text(outputKey), new DoubleWritable(outputValue));
    }
}