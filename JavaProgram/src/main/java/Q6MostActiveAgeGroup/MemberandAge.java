package Q6MostActiveAgeGroup;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 11. Most active age group
public class MemberandAge implements WritableComparable <MemberandAge> {

    // Key - Age Band
    // [LT-30)
    // [30-40)
    // [40-50)
    // [50+]
    private String ageBand;

    public MemberandAge() {
        super();
    }

    public MemberandAge(String band) {
        super();
        this.ageBand = band;
    }

    public String getAgeBand() {
        return ageBand;
    }

    public void setAgeBand(String ageBand) {
        this.ageBand = ageBand;
    }

    public int compareTo(MemberandAge o) {
        return this.ageBand.compareTo(o.getAgeBand());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(ageBand);
    }

    public void readFields(DataInput in) throws IOException {
        ageBand = in.readUTF();
    }

    @Override
    public String toString() {
        return ageBand;
    }
}
