package org.hadoop.manip3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AvgWorkHoursMapper extends Mapper<Object, Text, Text, NumPair> {

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String line=value.toString();
        String[] data=line.split(",");

        String maritalStatus=data[5];
        double hoursWorked=Double.parseDouble(data[12]);
        context.write(new Text(maritalStatus), new NumPair(hoursWorked, 1));

    }
}
