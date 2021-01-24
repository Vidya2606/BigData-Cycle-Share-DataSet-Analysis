package Q8Top5BusyRoutes;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FromToStationPair implements WritableComparable<FromToStationPair> {
    private String fromStationId;
    private String toStationId;

    public FromToStationPair() {
        super();
    }

    public FromToStationPair(String fromStationId, String toStationId) {
        super();
        this.fromStationId = fromStationId;
        this.toStationId = toStationId;
    }

    public String getFromStationId() {
        return fromStationId;
    }

    public void setFromStationId(String fromStationId) {
        this.fromStationId = fromStationId;
    }

    public String getToStationId() {
        return toStationId;
    }

    public void setToStationId(String toStationId) {
        this.toStationId = toStationId;
    }

    public int compareTo(FromToStationPair o) {
        int result = this.fromStationId.compareTo(o.fromStationId);
        if(result == 0) {
            return this.toStationId.compareTo(o.toStationId);
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(fromStationId);
        out.writeUTF(toStationId);
    }

    public void readFields(DataInput in) throws IOException {
        fromStationId = in.readUTF();
        toStationId = in.readUTF();
    }

    @Override
    public String toString() {
        return fromStationId + ", " + toStationId;
    }
}
