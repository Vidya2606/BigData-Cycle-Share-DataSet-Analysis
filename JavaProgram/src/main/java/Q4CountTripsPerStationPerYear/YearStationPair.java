package Q4CountTripsPerStationPerYear;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 7. Count of trips per station by year – Secondary Sorting
public class YearStationPair implements WritableComparable <YearStationPair> {

    private String year;
    private String station;

    public YearStationPair() {
        super();
    }

    public YearStationPair(String year, String station) {
        super();
        this.year = year;
        this.station = station;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public int compareTo(YearStationPair o) {
        int result = this.year.compareTo(o.year);
        if(result == 0) {
            return this.station.compareTo(o.station);
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(station);
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        station = in.readUTF();
    }

    @Override
    public String toString() {
        return year + "," + station;
    }
}
