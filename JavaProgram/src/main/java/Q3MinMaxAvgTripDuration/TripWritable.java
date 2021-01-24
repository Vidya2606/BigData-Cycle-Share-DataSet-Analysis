package Q3MinMaxAvgTripDuration;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 5. Min, Max and Average duration of trips from each station (by month-year)
public class TripWritable implements Writable {

    private double minTrip;
    private double maxTrip;
    private double avgTrip;
    private long count;

    public TripWritable() {
        super();
    }

    public TripWritable (double minTrip, double maxTrip, double avgTrip, long count) {
        super();
        this.minTrip = minTrip;
        this.maxTrip = maxTrip;
        this.avgTrip = avgTrip;
        this.count = count;
    }

    public double getMinTrip() {
        return minTrip;
    }

    public void setMinTrip(double minTrip) {
        this.minTrip = minTrip;
    }

    public double getMaxTrip() {
        return maxTrip;
    }

    public void setMaxTrip(double maxTrip) {
        this.maxTrip = maxTrip;
    }

    public double getAvgTrip() {
        return avgTrip;
    }

    public void setAvgTrip(double avgTrip) {
        this.avgTrip = avgTrip;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(minTrip);
        out.writeDouble(maxTrip);
        out.writeDouble(avgTrip);
        out.writeLong(count);
    }

    public void readFields(DataInput in) throws IOException {
        minTrip = in.readDouble();
        maxTrip = in.readDouble();
        avgTrip = in.readDouble();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return  "Min-Trip-Duration: " + minTrip + ", " +
                "Max-Trip-Duration: " + maxTrip + ", " +
                "Avg-Trip-Duration: " + avgTrip;
     }
}
