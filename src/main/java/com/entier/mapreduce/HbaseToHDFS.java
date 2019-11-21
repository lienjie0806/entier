package com.entier.mapreduce;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


public class HbaseToHDFS {
    private static final Logger logger = Logger.getLogger(HbaseToHDFS.class);
    public static Configuration conf = HBaseConfiguration.create();

    static
    {
        conf.set("hbase.zookeeper.property.clientPort", "11001");
        conf.set("hbase.zookeeper.quorum", "134.96.33.134,134.96.179.35,134.96.179.37");
        conf.set("hbase.master", "134.96.33.132:14001");
        conf.set("mapreduce.output.fileoutputformat.compress", "false");
    }

    public static void main(String[] args)
            throws Exception
    {

        String cloumns = "test,test2";
        conf.set("cloumns", cloumns);
        Job job = Job.getInstance(conf, "SingleTableAll");
        job.setJarByClass(HbaseToHDFS.class);

        job.setMapperClass(HbaseToHDFS.MyMapper.class);
        job.setNumReduceTasks(0);

        TableMapReduceUtil.initTableMapperJob(initScans(job, args[0]), HbaseToHDFS.MyMapper.class,
                NullWritable.class, Text.class, job);

        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(new Path(args[1]))) {
//            fs.delete(new Path(args[1]));
//        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        long start = System.currentTimeMillis();
        try
        {
            job.waitForCompletion(true);
        }
        finally
        {
            fs.setPermission(new Path(args[1]), new FsPermission("777"));
            FileStatus[] files = fs.listStatus(new Path(args[1]));
            for (FileStatus fileStatus : files)
            {
                Path p = fileStatus.getPath();
                fs.setPermission(p, new FsPermission("777"));
            }
            fs.close();

            long end = System.currentTimeMillis();
            logger.info("Job<" + job.getJobName() + ">是否执行成功:" + job.isSuccessful() + ";开始时间:" + start + "; 结束时间:" + end + "; 用时:" + (end - start) + "ms");
        }
    }

    private static List<Scan> initScans(Job job, String tableName)
    {
        Configuration conf = job.getConfiguration();
        Scan scan = new Scan();
        scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(tableName));

        return Lists.newArrayList(new Scan[] { scan });
    }

    public static class MyMapper
            extends TableMapper<NullWritable, Text>
    {
        String cloumns = "";

        protected void map(ImmutableBytesWritable key, Result r, Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context)
                throws IOException, InterruptedException
        {
            if (r != null)
            {
                String all = "";
                int j = 0;
                for (String cloumn : this.cloumns.split(","))
                {
                    j++;
                    String s = "";
                    try
                    {
                        byte[] p = r.getValue("cf1".getBytes(), cloumn.getBytes());
                        if (p != null)
                        {
                            s = new String(p, "UTF-8");

                            s = s.replaceAll("\\n", "").replaceAll("\\r", "");
                            s = s.replaceAll(",", ".");
                            s = s.replaceAll(";", ".");
                            if ("NULL".equals(s)) {
                                s = "";
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        System.out.println("111");
                        s = "";
                    }
                    if (j == 1) {
                        all = s;
                    } else {
                        all = all + "," + s;
                    }
                }
                context.write(NullWritable.get(), new Text(all));
            }
        }

        protected void setup(Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context)
                throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            this.cloumns = conf.get("cloumns");
        }
    }
}
