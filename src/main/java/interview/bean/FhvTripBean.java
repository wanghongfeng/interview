package interview.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

@lombok.Data
public class FhvTripBean implements Writable, Serializable {
    private String hvfhslicensenum;
    private String dispatchingBaseNum;
    private String pickupDatetime;
    private String dropoffDatetime;
    private String pulocationid;
    private String dolocationid;
    private String srFlag;
    private String dispatchingBaseNumber;





    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(hvfhslicensenum);
        dataOutput.writeUTF(dispatchingBaseNum);
        dataOutput.writeUTF(pickupDatetime);
        dataOutput.writeUTF(dropoffDatetime);
        dataOutput.writeUTF(pulocationid);
        dataOutput.writeUTF(dolocationid);
        dataOutput.writeUTF(srFlag);
        dataOutput.writeUTF(dispatchingBaseNumber);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hvfhslicensenum = dataInput.readUTF();
        this.dispatchingBaseNum = dataInput.readUTF();
        this.pickupDatetime = dataInput.readUTF();
        this.dropoffDatetime = dataInput.readUTF();
        this.pulocationid = dataInput.readUTF();
        this.dolocationid = dataInput.readUTF();
        this.srFlag = dataInput.readUTF();
        this.dispatchingBaseNumber = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return this.getHvfhslicensenum()+","+

                this.getPickupDatetime()+","+
                this.getDropoffDatetime()+","+
                this.getPulocationid()+","+
                this.getDolocationid()+","+
                this.getSrFlag()+","+
                this.getDispatchingBaseNumber();
    }
}
