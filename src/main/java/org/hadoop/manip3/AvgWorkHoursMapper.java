package org.hadoop.manip3;

import org.apache.hadoop.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AvgWorkHoursMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String line=value.toString();
        String[] data=line.split(",");

        String maritalStatus=data[5];
        double hrs=Double.parseDouble(data[12]);
        context.write(new Text(maritalStatus), new Text(hrs+",1"));

    }
}
