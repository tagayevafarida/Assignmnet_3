package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Main.class);
        job.setJobName("Earthquake measurment");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(EarthquakeMapper.class);
        job.setReducerClass(EarthquakeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}