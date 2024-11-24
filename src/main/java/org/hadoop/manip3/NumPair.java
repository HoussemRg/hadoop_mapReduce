package org.hadoop.manip3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumPair implements Writable {
    private double sum;
    private int count;

    public NumPair() {}

    public NumPair(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public void set(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return sum + "," + count;
    }
}
