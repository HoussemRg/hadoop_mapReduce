package org.hadoop.manip3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class AvgWorkHoursCombiner extends Reducer<Text, NumPair, Text, NumPair> {

    public void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (NumPair pair : values) {
            sum += pair.getSum();
            count += pair.getCount();
        }

        context.write(key, new NumPair(sum, count));
    }
}
