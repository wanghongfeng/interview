package interview;


import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class hello {
    public static Map<String ,Integer> map=new HashMap<String, Integer>();

    public static void main(String[] args) {
        String a="-73.9833602905273";
        double b = Double.valueOf(a);
        DecimalFormat df = new DecimalFormat("#.0000000000000");
        String temp = df.format(b);
        b = Double.valueOf(temp);

        if (Long.valueOf("4.81")<0.01){
            System.out.println("fff");
        }

    }
}
