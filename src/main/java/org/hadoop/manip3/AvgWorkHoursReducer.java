package org.hadoop.manip3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgWorkHoursReducer extends Reducer<Text, NumPair, Text, Text> {

    public void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        double totalSum = 0;
        int totalCount = 0;

        for (NumPair pair : values) {
            totalSum += pair.getSum();
            totalCount += pair.getCount();
        }

        // Calculer la moyenne
        double average = totalCount > 0 ? totalSum / totalCount : 0;
        context.write(key, new Text(String.format("%.2f", average)));
    }
}
