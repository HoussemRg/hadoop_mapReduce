package org.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.manip3.AvgWorkHoursCombiner;
import org.hadoop.manip3.AvgWorkHoursMapper;
import org.hadoop.manip3.AvgWorkHoursReducer;
import org.hadoop.manip3.NumPair;

public class AverageCalculator {
    public static void main(String[] args)  {

        if (args.length != 2) {
            System.err.println("Usage: AverageCalculator <input path> <output path>");
            System.exit(-1);
        }

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Average calculation");

            job.setJarByClass(AverageCalculator.class);
            job.setMapperClass(AvgWorkHoursMapper.class);
            job.setCombinerClass(AvgWorkHoursCombiner.class);
            job.setReducerClass(AvgWorkHoursReducer.class);

            // Configurer les types
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NumPair.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}