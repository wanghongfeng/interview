package interview.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

@lombok.Data
public class YellowGreenTripBean implements Writable, Serializable {
    private String vendorid;
    private String vendorName;
    private String tpepPickupDatetime;
    private String tpepDropoffDatetime;
    private String passengerCount;
    private String tripDistance;
    private String pulocationid;
    private String pickupLongitude;
    private String pickupLatitude;
    private String ratecodeid;
    private String storeAndFwdFlag;
    private String dolocationid;
    private String dropoffLongitude;
    private String dropoffLatitude;
    private String paymentType;
    private String fareAmount;
    private String extra;
    private String mtaTax;
    private String tipAmount;
    private String tollsAmount;
    private String improvementSurcharge;
    private String totalAmount;
    private String congestionSurcharge;
    private String tripType;
    private String ehailFee;
    private String speed;
    private String timeDiffMin;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(vendorid);
        dataOutput.writeUTF(vendorName);
        dataOutput.writeUTF(tpepPickupDatetime);
        dataOutput.writeUTF(tpepDropoffDatetime);
        dataOutput.writeUTF(passengerCount);
        dataOutput.writeUTF(tripDistance);
        dataOutput.writeUTF(pulocationid);
        dataOutput.writeUTF(pickupLongitude);
        dataOutput.writeUTF(pickupLatitude);
        dataOutput.writeUTF(ratecodeid);
        dataOutput.writeUTF(storeAndFwdFlag);
        dataOutput.writeUTF(dolocationid);
        dataOutput.writeUTF(dropoffLongitude);
        dataOutput.writeUTF(dropoffLatitude);
        dataOutput.writeUTF(paymentType);
        dataOutput.writeUTF(fareAmount);
        dataOutput.writeUTF(extra);
        dataOutput.writeUTF(mtaTax);
        dataOutput.writeUTF(tipAmount);
        dataOutput.writeUTF(tollsAmount);
        dataOutput.writeUTF(improvementSurcharge);
        dataOutput.writeUTF(totalAmount);
        dataOutput.writeUTF(congestionSurcharge);
        dataOutput.writeUTF(tripType);
        dataOutput.writeUTF(ehailFee);
        dataOutput.writeUTF(speed);
        dataOutput.writeUTF(timeDiffMin);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vendorid = dataInput.readUTF();
        this.vendorName = dataInput.readUTF();
        this.tpepPickupDatetime = dataInput.readUTF();
        this.tpepDropoffDatetime = dataInput.readUTF();
        this.passengerCount = dataInput.readUTF();
        this.tripDistance = dataInput.readUTF();
        this.pulocationid = dataInput.readUTF();
        this.pickupLongitude = dataInput.readUTF();
        this.pickupLatitude = dataInput.readUTF();
        this.ratecodeid = dataInput.readUTF();
        this.storeAndFwdFlag = dataInput.readUTF();
        this.dolocationid = dataInput.readUTF();
        this.dropoffLongitude = dataInput.readUTF();
        this.dropoffLatitude = dataInput.readUTF();
        this.paymentType = dataInput.readUTF();
        this.fareAmount = dataInput.readUTF();
        this.extra = dataInput.readUTF();
        this.mtaTax = dataInput.readUTF();
        this.tipAmount = dataInput.readUTF();
        this.tollsAmount = dataInput.readUTF();
        this.improvementSurcharge = dataInput.readUTF();
        this.totalAmount = dataInput.readUTF();
        this.congestionSurcharge = dataInput.readUTF();
        this.tripType = dataInput.readUTF();
        this.ehailFee = dataInput.readUTF();
        this.speed = dataInput.readUTF();
        this.timeDiffMin = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.getVendorid()+","+
                this.getVendorName()+","+
                this.getTpepPickupDatetime()+","+
                this.getTpepDropoffDatetime()+","+
                this.getPassengerCount()+","+
                this.getTripDistance()+","+
                this.getPulocationid()+","+
                this.getPickupLongitude()+","+
                this.getPickupLatitude()+","+
                this.getRatecodeid()+","+
                this.getStoreAndFwdFlag()+","+
                this.getDolocationid()+","+
                this.getDropoffLongitude()+","+
                this.getDropoffLatitude()+","+
                this.getPaymentType()+","+
                this.getFareAmount()+","+
                this.getExtra()+","+
                this.getMtaTax()+","+
                this.getTipAmount()+","+
                this.getTollsAmount()+","+
                this.getImprovementSurcharge()+","+
                this.getTotalAmount()+","+
                this.getCongestionSurcharge()+","+
                this.getTripType()+","+
                this.getEhailFee()+","+
                this.getSpeed()+","+
                this.getTimeDiffMin();

    }
}
