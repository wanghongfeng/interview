package interview.partition;

import interview.bean.YellowGreenTripBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YellowTripPartition extends Partitioner<Text, YellowGreenTripBean> {
    @Override
    public int getPartition(Text monthNum, YellowGreenTripBean flowBean, int i) {
        int fileNameStr=Integer.parseInt(monthNum.toString());
        if(fileNameStr<200909){
            return  0;
        }else if(fileNameStr<201005){
            return  1;
        }else if(fileNameStr<201012){
            return  2;
        }else if(fileNameStr<201109){
            return  3;
        }else if(fileNameStr<201212){
            return  4;
        }else if(fileNameStr<201405){
            return  5;
        }else if(fileNameStr<201502){
            return  6;
        }else if(fileNameStr<201513){
            return  7;
        }else if(fileNameStr<201705){
            return  8;
        }else if(fileNameStr<201910){
            return  9;
        }else{
            return  10;
        }
    }
}
