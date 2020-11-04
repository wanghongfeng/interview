package interview;

import interview.mapper.ReadStructureMapper;
import interview.partition.ReadStructurePartition;
import interview.reducer.ReadStructureReducer;
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

import java.net.URLDecoder;

public class ReadStructure extends Configured  implements Tool
{
    @Override
    public int run(String[] strings) throws Exception {
        //创建一个任务对象


        String[] otherArgs = new GenericOptionsParser(this.getConf(), strings).getRemainingArgs();

        this.getConf().set("mapreduce.output.textoutputformat.separator",",");

        Job job = Job.getInstance(super.getConf(), "mapreduce_flowcount_partition");

        //打包放在集群运行时，需要做一个配置
        job.setJarByClass(ReadStructure.class);
        //第一步:设置读取文件的类: K1 和V1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(URLDecoder.decode("s3://nyc-tlc/trip data/", "UTF-8")));
//        TextInputFormat.addInputPath(job, new Path(URLDecoder.decode("s3://my-wang-wordcount/input/", "UTF-8")));


        //第二步：设置Mapper类
        job.setMapperClass(ReadStructureMapper.class);
        //设置Map阶段的输出类型: k2 和V2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //第三,四，五，六步采用默认方式(分区，排序，规约，分组)
        job.setPartitionerClass(ReadStructurePartition.class);

        //第七步 ：设置文的Reducer类
        job.setReducerClass(ReadStructureReducer.class);
        //设置Reduce阶段的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置Reduce的个数
        job.setNumReduceTasks(4);
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

        int run = ToolRunner.run(configuration, new ReadStructure(), args);
        System.exit(run);
    }

}
