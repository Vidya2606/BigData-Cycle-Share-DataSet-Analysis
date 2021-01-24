package Q5Top5BusiestStations;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StationCountWrittable implements Writable {

    private String station;
    private Integer count;

    public StationCountWrittable() {
        super();
    }

    public StationCountWrittable(String station, Integer count) {
        super();
        this.station = station;
        this.count = count;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(station);
        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        station = in.readUTF();
        count = in.readInt();
    }

    public String toString() {
        return station + ", " + count;
    }
}
