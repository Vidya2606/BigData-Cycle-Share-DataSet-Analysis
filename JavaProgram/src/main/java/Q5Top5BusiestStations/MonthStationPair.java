package Q5Top5BusiestStations;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 8.Top 5 most incoming stations by month

public class MonthStationPair implements WritableComparable <MonthStationPair> {
    private String month;
    private String station;

    public MonthStationPair() {
        super();
    }

    public MonthStationPair(String month, String staion) {
        super();
        this.month = month;
        this.station = staion;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public int compareTo(MonthStationPair o) {
        int result = this.month.compareTo(o.month);
        if(result == 0) {
            return this.station.compareTo(o.station);
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(month);
        out.writeUTF(station);
    }

    public void readFields(DataInput in) throws IOException {
        month = in.readUTF();
        station = in.readUTF();
    }

    @Override
    public String toString() {
        return month + ", " + station;
    }
}
