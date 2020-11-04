package interview;

import interview.bean.YellowGreenTripBean;
import interview.mapper.YellowTripMapper;
import interview.partition.YellowTripPartition;
import interview.reducer.YellowTripReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured  implements Tool
{
    @Override
    public int run(String[] strings) throws Exception {
        //创建一个任务对象


        String[] otherArgs = new GenericOptionsParser(this.getConf(), strings).getRemainingArgs();

        this.getConf().set("mapreduce.output.textoutputformat.separator",",");

        Job job = Job.getInstance(super.getConf(), "mapreduce_flowcount_partition");

        //打包放在集群运行时，需要做一个配置
        job.setJarByClass(JobMain.class);
        //第一步:设置读取文件的类: K1 和V1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://nyc-tlc/trip data/"));
//        TextInputFormat.addInputPath(job, new Path("s3://my-wang-wordcount/input/"));

        //第二步：设置Mapper类
        job.setMapperClass(YellowTripMapper.class);
        //设置Map阶段的输出类型: k2 和V2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(YellowGreenTripBean.class);

        //第三,四，五，六步采用默认方式(分区，排序，规约，分组)
        job.setPartitionerClass(YellowTripPartition.class);

        //第七步 ：设置文的Reducer类
        job.setReducerClass(YellowTripReducer.class);
        //设置Reduce阶段的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(YellowGreenTripBean.class);

        //设置Reduce的个数
        job.setNumReduceTasks(11);
        //第八步:设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径

        TextOutputFormat.setOutputPath(job, new Path(otherArgs[0]));


        boolean b = job.waitForCompletion(true);
        return b?0:1;

    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动一个任务

        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }

}
