package org.hadoop.manip3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgWorkHoursReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            sum += Double.parseDouble(parts[0]);
            count += Integer.parseInt(parts[1]);
        }

        double average = sum / count;
        context.write(key, new Text(String.valueOf(average)));
    }
}
