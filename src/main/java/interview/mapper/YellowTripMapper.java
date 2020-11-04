package interview.mapper;

import interview.bean.YellowGreenTripBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class YellowTripMapper extends Mapper<LongWritable,Text,Text, YellowGreenTripBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //首行跳过
        if (key.toString().equals("0")) {
            return;
        }
        //提取文件名
        String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
        int endint=fileName.indexOf(".csv");
        int monthNum=Integer.parseInt(fileName.substring(endint-7,endint).replace("-",""));

        String[] split = value.toString().split(",");
        if (split.length<10){
            return;
        }
        YellowGreenTripBean flowBean=new YellowGreenTripBean();
        if(fileName.indexOf("yellow")>-1){
            if (!(structureYellow(flowBean,monthNum,split))){
                return;
            }
        }else if(fileName.indexOf("green")>-1){
            if (!(structureGreen(flowBean,monthNum,split))){
                return;
            }
        }else {
            return;
        }
        if (!checkData(flowBean)){
            return;
        }
        context.write(new Text(monthNum+""), flowBean);
    }
    public static boolean checkData(YellowGreenTripBean flowBean){
        try {
            DecimalFormat df = new DecimalFormat("#.00");

            if (Double.valueOf(flowBean.getTotalAmount())<0.01){
                return false;
            }
            long mins=getDistanceTimes(flowBean.getTpepPickupDatetime(),flowBean.getTpepDropoffDatetime());
            if (mins<1){
                return false;
            }
            flowBean.setTimeDiffMin(df.format(mins));
            Double speed=(Double.valueOf(flowBean.getTripDistance())/mins)*60;
            if(speed<0.01) {
                return false;
            }
            flowBean.setSpeed(df.format(speed));
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public static long getDistanceTimes(String begin, String end) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long diff = 0;
        try {
            diff = df.parse(end).getTime() - df.parse(begin).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return diff/(60 * 1000) ;
    }
    public static boolean structureYellow(YellowGreenTripBean flowBean, int monthNum, String[] split){
        if (monthNum<200913){
            if (split.length<18){
                return false;
            }
            flowBean.setVendorid("");
            flowBean.setVendorName(split[0]);
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setPassengerCount(split[3]);
            flowBean.setTripDistance(split[4]);
            flowBean.setPulocationid("");//待给出
            flowBean.setPickupLongitude(split[5]);
            flowBean.setPickupLatitude(split[6]);
            flowBean.setRatecodeid(split[7]);
            flowBean.setStoreAndFwdFlag(split[8]);
            flowBean.setDolocationid("");//待给出
            flowBean.setDropoffLongitude(split[9]);
            flowBean.setDropoffLatitude(split[10]);
            flowBean.setPaymentType(split[11]);
            flowBean.setFareAmount(split[12]);
            flowBean.setExtra(split[13]);
            flowBean.setMtaTax(split[14]);
            flowBean.setTipAmount(split[15]);
            flowBean.setTollsAmount(split[16]);
            flowBean.setImprovementSurcharge("");
            flowBean.setTotalAmount(split[17]);
            flowBean.setCongestionSurcharge("");
            flowBean.setTripType("");
            flowBean.setEhailFee("");
        }else if (monthNum<201413) {
            if (split.length<18){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setPassengerCount(split[3]);
            flowBean.setTripDistance(split[4]);
            flowBean.setPulocationid("");
            flowBean.setPickupLongitude(split[5]);
            flowBean.setPickupLatitude(split[6]);
            flowBean.setRatecodeid(split[7]);
            flowBean.setStoreAndFwdFlag(split[8]);
            flowBean.setDolocationid("");
            flowBean.setDropoffLongitude(split[9]);
            flowBean.setDropoffLatitude(split[10]);
            flowBean.setPaymentType(split[11]);
            flowBean.setFareAmount(split[12]);
            flowBean.setExtra(split[13]);
            flowBean.setMtaTax(split[14]);
            flowBean.setTipAmount(split[15]);
            flowBean.setTollsAmount(split[16]);
            flowBean.setImprovementSurcharge("");
            flowBean.setTotalAmount(split[17]);
            flowBean.setCongestionSurcharge("");
            flowBean.setTripType("");
            flowBean.setEhailFee("");
        }else if (monthNum<201607) {
            if (split.length<19){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setPassengerCount(split[3]);
            flowBean.setTripDistance(split[4]);
            flowBean.setPulocationid("");
            flowBean.setPickupLongitude(split[5]);
            flowBean.setPickupLatitude(split[6]);
            flowBean.setRatecodeid(split[7]);
            flowBean.setStoreAndFwdFlag(split[8]);
            flowBean.setDolocationid("");
            flowBean.setDropoffLongitude(split[9]);
            flowBean.setDropoffLatitude(split[10]);
            flowBean.setPaymentType(split[11]);
            flowBean.setFareAmount(split[12]);
            flowBean.setExtra(split[13]);
            flowBean.setMtaTax(split[14]);
            flowBean.setTipAmount(split[15]);
            flowBean.setTollsAmount(split[16]);
            flowBean.setImprovementSurcharge(split[17]);
            flowBean.setTotalAmount(split[18]);
            flowBean.setCongestionSurcharge("");
            flowBean.setTripType("");
            flowBean.setEhailFee("");
        }else if (monthNum<201813) {
            if (split.length<17){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setPassengerCount(split[3]);
            flowBean.setTripDistance(split[4]);
            flowBean.setPulocationid(split[7]);
            flowBean.setPickupLongitude("");
            flowBean.setPickupLatitude("");
            flowBean.setRatecodeid(split[5]);
            flowBean.setStoreAndFwdFlag(split[6]);
            flowBean.setDolocationid(split[8]);
            flowBean.setDropoffLongitude("");
            flowBean.setDropoffLatitude("");
            flowBean.setPaymentType(split[9]);
            flowBean.setFareAmount(split[10]);
            flowBean.setExtra(split[11]);
            flowBean.setMtaTax(split[12]);
            flowBean.setTipAmount(split[13]);
            flowBean.setTollsAmount(split[14]);
            flowBean.setImprovementSurcharge(split[15]);
            flowBean.setTotalAmount(split[16]);
            flowBean.setCongestionSurcharge("");
            flowBean.setTripType("");
            flowBean.setEhailFee("");
        }else if (monthNum<202007) {
            if (split.length<18){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setPassengerCount(split[3]);
            flowBean.setTripDistance(split[4]);
            flowBean.setPulocationid(split[7]);
            flowBean.setPickupLongitude("");
            flowBean.setPickupLatitude("");
            flowBean.setRatecodeid(split[5]);
            flowBean.setStoreAndFwdFlag(split[6]);
            flowBean.setDolocationid(split[8]);
            flowBean.setDropoffLongitude("");
            flowBean.setDropoffLatitude("");
            flowBean.setPaymentType(split[9]);
            flowBean.setFareAmount(split[10]);
            flowBean.setExtra(split[11]);
            flowBean.setMtaTax(split[12]);
            flowBean.setTipAmount(split[13]);
            flowBean.setTollsAmount(split[14]);
            flowBean.setImprovementSurcharge(split[15]);
            flowBean.setTotalAmount(split[16]);
            flowBean.setCongestionSurcharge(split[17]);
            flowBean.setTripType("");
            flowBean.setEhailFee("");
        }

        return true;
    }

    public static boolean structureGreen(YellowGreenTripBean flowBean, int monthNum, String[] split){
        if (monthNum<201413){
            if (split.length<20){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setStoreAndFwdFlag(split[3]);
            flowBean.setRatecodeid(split[4]);
            flowBean.setPulocationid("");//待给出
            flowBean.setPickupLongitude(split[5]);
            flowBean.setPickupLatitude(split[6]);
            flowBean.setDolocationid("");//待给出
            flowBean.setDropoffLongitude(split[7]);
            flowBean.setDropoffLatitude(split[8]);
            flowBean.setPassengerCount(split[9]);
            flowBean.setTripDistance(split[10]);
            flowBean.setFareAmount(split[11]);
            flowBean.setExtra(split[12]);
            flowBean.setMtaTax(split[13]);
            flowBean.setTipAmount(split[14]);
            flowBean.setTollsAmount(split[15]);
            flowBean.setEhailFee(split[16]);
            flowBean.setTotalAmount(split[17]);
            flowBean.setPaymentType(split[18]);
            flowBean.setTripType(split[19]);
            flowBean.setImprovementSurcharge("");
            flowBean.setCongestionSurcharge("");
        }else if (monthNum<201607) {
            if (split.length<21){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setStoreAndFwdFlag(split[3]);
            flowBean.setRatecodeid(split[4]);
            flowBean.setPulocationid("");//待给出
            flowBean.setPickupLongitude(split[5]);
            flowBean.setPickupLatitude(split[6]);
            flowBean.setDolocationid("");//待给出
            flowBean.setDropoffLongitude(split[7]);
            flowBean.setDropoffLatitude(split[8]);
            flowBean.setPassengerCount(split[9]);
            flowBean.setTripDistance(split[10]);
            flowBean.setFareAmount(split[11]);
            flowBean.setExtra(split[12]);
            flowBean.setMtaTax(split[13]);
            flowBean.setTipAmount(split[14]);
            flowBean.setTollsAmount(split[15]);
            flowBean.setEhailFee(split[16]);
            flowBean.setImprovementSurcharge(split[17]);
            flowBean.setTotalAmount(split[18]);
            flowBean.setPaymentType(split[19]);
            flowBean.setTripType(split[20]);
            flowBean.setCongestionSurcharge("");
        }else if (monthNum<201813) {
            if (split.length<19){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setStoreAndFwdFlag(split[3]);
            flowBean.setRatecodeid(split[4]);
            flowBean.setPulocationid(split[5]);//待给出
            flowBean.setPickupLongitude("");
            flowBean.setPickupLatitude("");
            flowBean.setDolocationid(split[6]);//待给出
            flowBean.setDropoffLongitude("");
            flowBean.setDropoffLatitude("");
            flowBean.setPassengerCount(split[7]);
            flowBean.setTripDistance(split[8]);
            flowBean.setFareAmount(split[9]);
            flowBean.setExtra(split[10]);
            flowBean.setMtaTax(split[11]);
            flowBean.setTipAmount(split[12]);
            flowBean.setTollsAmount(split[13]);
            flowBean.setEhailFee(split[14]);
            flowBean.setImprovementSurcharge(split[15]);
            flowBean.setTotalAmount(split[16]);
            flowBean.setPaymentType(split[17]);
            flowBean.setTripType(split[18]);
            flowBean.setCongestionSurcharge("");
        }else if (monthNum<202007) {
            if (split.length<20){
                return false;
            }
            flowBean.setVendorid(split[0]);
            flowBean.setVendorName("");
            flowBean.setTpepPickupDatetime(split[1]);
            flowBean.setTpepDropoffDatetime(split[2]);
            flowBean.setStoreAndFwdFlag(split[3]);
            flowBean.setRatecodeid(split[4]);
            flowBean.setPulocationid(split[5]);//待给出
            flowBean.setPickupLongitude("");
            flowBean.setPickupLatitude("");
            flowBean.setDolocationid(split[6]);//待给出
            flowBean.setDropoffLongitude("");
            flowBean.setDropoffLatitude("");
            flowBean.setPassengerCount(split[7]);
            flowBean.setTripDistance(split[8]);
            flowBean.setFareAmount(split[9]);
            flowBean.setExtra(split[10]);
            flowBean.setMtaTax(split[11]);
            flowBean.setTipAmount(split[12]);
            flowBean.setTollsAmount(split[13]);
            flowBean.setEhailFee(split[14]);
            flowBean.setImprovementSurcharge(split[15]);
            flowBean.setTotalAmount(split[16]);
            flowBean.setPaymentType(split[17]);
            flowBean.setTripType(split[18]);
            flowBean.setCongestionSurcharge(split[19]);
        }

        return true;
    }
}
