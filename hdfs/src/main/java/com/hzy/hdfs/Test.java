package com.hzy.hdfs;

import com.hzy.entity.ChineseMedicine;
import com.hzy.entity.EmploeeWritable;
import com.hzy.entity.MyText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.rmi.log.LogInputStream;

import java.io.IOException;
import java.util.*;

/**
 * Created by Hzy on 2016/1/21.
 */
public class Test {
    public static String hdfsUrl = "hdfs://192.168.189.142:8020";
    public static String pathfile = "/tmp/hdfs/test.txt";
    public static String pathfilein = "H:\\java\\test\\in\\testin.txt";
    public static String pathfileout = "H:\\java\\test\\out\\testout.txt";
    public static String path = "/tmp/hdfs";

    public static void main(String[] args) throws IOException, Exception {
        try {

            //Configuration conf = new Configuration();
            //FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
            //Path file = new Path(pathfile);
            //Path filein = new Path(pathfilein);
            //Path fileout = new Path(pathfileout);
            //HdfsUtil.readFile(fs, file);
            //HdfsUtil.createWriteFile(fs,file,"test");
            //fs.close();
            Configuration conf = new Configuration();
        /*
        第一个job
         */

            Job job = Job.getInstance(conf, "price");
            job.setJarByClass(Test.class);

            job.setMapperClass(Map.class);
            //job.setCombinerClass(Reduce.class);
            job.setReducerClass(Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //job.setSortComparatorClass();

            //map阶段的输出的key
            //job.setOutputKeyClass(Text.class);
            //map阶段的输出的value
            //job.setOutputValueClass(Text.class);

            //加入控制容器
            ControlledJob ctrljob1 = new ControlledJob(conf);
            ctrljob1.setJob(job);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //System.exit(job.waitForCompletion(true) ? 0 : 1);


            Job job2 = Job.getInstance(conf, "order total");
            job2.setJarByClass(Test.class);

            job2.setMapperClass(Map1.class);
            job2.setReducerClass(Reduce1.class);

            //map阶段的输出的key
            job2.setOutputKeyClass(Text.class);
            //map阶段的输出的value
            job2.setOutputValueClass(Text.class);

            job2.setSortComparatorClass(NameComparator.class);

            //作业2加入控制容器
            ControlledJob ctrljob2 = new ControlledJob(conf);
            ctrljob2.setJob(job2);

            //设置多个作业直接的依赖关系
            //如下所写：
            //意思为job2的启动，依赖于job1作业的完成
            ctrljob2.addDependingJob(ctrljob1);


            //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
            FileInputFormat.addInputPath(job2, new Path(args[1]));

            //输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
            //因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "1"));


            //主的控制容器，控制上面的总的两个子作业
            JobControl jobCtrl = new JobControl("myctrl");

            //添加到总的JobControl里，进行控制
            jobCtrl.addJob(ctrljob1);
            jobCtrl.addJob(ctrljob2);

            //在线程启动，记住一定要有这个
            Thread t = new Thread(jobCtrl);
            t.start();

            while (true) {

                if (jobCtrl.allFinished()) {//如果作业成功完成，就打印成功作业的信息
                    System.out.println(jobCtrl.getSuccessfulJobList());
                    jobCtrl.stop();
                    break;
                }
            }
            //System.exit(job2.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.out.println("Got a Exception：" + e.getMessage());
            e.printStackTrace();
            //throw e;    //不做进一步处理，将异常向外抛出
        }


    }


    //负责根据名称作为Key，吧价格集中
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private static Text line = new Text();//每行数据

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            //获取名称
            String name = line.toString().split(" ")[0];
            //获取价格
            Integer price = Integer.parseInt(line.toString().split(" ")[1]);
//            System.out.println("line:" + line);
//            System.out.println("name:" + name + "price:" + price);
            //context.write(line, new Text(""));
            context.write(new Text(name), new IntWritable(price));
        }
    }


    //求最大价格
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        //实现reduce函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float maxPrice = Integer.MIN_VALUE;
            float mixPrice = Integer.MAX_VALUE;
            float avgPrice = 0;
            int total = 0;
            List<ChineseMedicine> chineseMedicineList = new ArrayList<>();
            ArrayList<ChineseMedicine> list = new ArrayList<ChineseMedicine>();
            for (IntWritable value : values) {
                maxPrice = Math.max(maxPrice, value.get());
                mixPrice = Math.min(mixPrice, value.get());
                avgPrice += value.get();
                total++;
            }
            avgPrice = avgPrice / total;

            ChineseMedicine chineseMedicine = new ChineseMedicine();
            chineseMedicine.setName(key.toString());
            chineseMedicine.setMaxPrice(maxPrice);
            chineseMedicine.setMinPrice(mixPrice);
            chineseMedicine.setAvgPrice(avgPrice);
            chineseMedicine.setTotal(total);
            list.add(chineseMedicine);

            //System.out.println("list.size()="+list.size());



            //组装结果
/*
            ArrayList<ChineseMedicine> result = new ArrayList<ChineseMedicine>();
            for (ChineseMedicine r1 : list) {
                System.out.println("r1.getName()"+r1.getName()+"r1.getTotal()"+r1.getTotal());
            }
*/

            for (ChineseMedicine r : list) {
                key.set(r.getName());
                String value = r.getMinPrice() + "\t"
                        + r.getMaxPrice() + "\t"
                        + r.getAvgPrice() + "\t"
                        + r.getTotal() + "\t";
                context.write(key, new Text(value));
            }

/*
            chineseMedicine.setName(key.toString());
            chineseMedicine.setMaxPrice(maxPrice);
            chineseMedicine.setMinPrice(mixPrice);
            chineseMedicine.setAvgPrice(avgPrice);
            chineseMedicine.setTotal(total);
            chineseMedicineList.add(chineseMedicine);
            context.write(key, chineseMedicine);*/
        }
    }

    //负责根据名称作为Key，吧价格集中
    public static class Map1 extends Mapper<Object, Text, Text, Text> {
        private static Text name = new Text();//每行数据
        private static Text row = new Text();//每行数据

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //line = key;
            //line2 = value;
            //System.out.println("line:" + line);
            //System.out.println("key.getLength():" + key.getLength());
            //System.out.println("value.getLength():" + value.getLength());

            //System.out.println("key:" + key);
            //获取名称
            String[] splits = value.toString().split("\t");
            //System.out.println("splits.length=" + splits.length);
            name.set(splits[4]+ "\t" + splits[0] );
            context.write(name, value);

          /*  System.out.println("splits[0]=" + splits[0]);
            System.out.println("splits[1]=" + splits[1]);
            String[] prices = splits[1].replace("[","").replace("]","").split(",");
            List<FloatWritable> list = new ArrayList<>();
            for(String s:prices){
                list.add(new FloatWritable(Float.parseFloat(s)));
                //context.write(new Text(splits[0]), new FloatWritable(Float.parseFloat(s)));
            }

            context.write(new Text(splits[0]), list);*/


        }
    }


    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        //实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("values=" + values);
            //System.out.println("key=" + key);
            List<ChineseMedicine> list = new ArrayList<>();

            for (Text row : values) {

                //System.out.println("row.toString()"+row.toString());
                String[] items = row.toString().split("\t");
                ChineseMedicine chineseMedicine = new ChineseMedicine();
                chineseMedicine.setName(items[0]);
                chineseMedicine.setMinPrice(Float.parseFloat(items[1]));
                chineseMedicine.setMaxPrice(Float.parseFloat(items[2]));
                chineseMedicine.setAvgPrice(Float.parseFloat(items[3]));
                chineseMedicine.setTotal(Integer.parseInt(items[4]));
                list.add(chineseMedicine);
            }

            //排序
/*            Collections.sort(list, new Comparator<ChineseMedicine>() {
                public int compare(ChineseMedicine r1, ChineseMedicine r2) {
                    System.out.println("r1.getName()" + r1.getName() + "r1.getTotal()" + r1.getTotal());
                    System.out.println("r2.getName()" + r2.getName() + "r2.getTotal()" + r2.getTotal());
                    return (r1.getTotal().compareTo(r2.getTotal()));
                }
            });*/

            for (ChineseMedicine r : list) {
                key.set(r.getName());
                String value = r.getMinPrice() + "\t"
                        + r.getMaxPrice() + "\t"
                        + r.getAvgPrice() + "\t"
                        + r.getTotal() + "\t";
                context.write(key, new Text(value));
            }

     /*       System.out.println("reduce");
            float avgPrice = 0;
            List<FloatWritable> listPrice = new ArrayList<>();
            listPrice.add(new FloatWritable(avgPrice));*/
            //context.write(key, listPrice);
        }
    }


    public static class Comparator extends WritableComparator{
        private static final Text.Comparator TEXT_COMPARATOR= new Text.Comparator();

        protected Comparator() {
            super(EmploeeWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                /**
                 * name是Text类型，Text是标准的UTF-8字节流，
                 * 由一个变长整形开头表示Text中文本所需要的长度，接下来就是文本本身的字节数组
                 * decodeVIntSize返回变长整形的长度，readVInt表示文本字节数组的长度，加起来就是第一个成员name的长度
                 */
                int nameL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int nameL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                //System.out.println("nameL1=" + nameL1 + "nameL2=" + nameL2);
                //和compareTo方法一样，先比较name
                int cmp = TEXT_COMPARATOR.compare(b1, s1, nameL1, b2, s2, nameL2);
                //System.out.println("cmp=" + cmp);
                if (cmp != 0) {
                    return cmp;
                }
                //再比较role
                return TEXT_COMPARATOR.compare(b1, s1 + nameL1, l1 - nameL1, b2, s2 + nameL2, l2 - nameL2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }
        }

        static {
            //注册raw comprator,更象是绑定，这样MapReduce使用EmploeeWritable时就会直接调用Comparator
            WritableComparator.define(EmploeeWritable.class,new Comparator());
        }
    }

    public static class NameComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        protected NameComparator() {
            super(EmploeeWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            System.out.println("进的第一个");
            try {
                int nameL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int nameL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return -TEXT_COMPARATOR.compare(b1, s1, nameL1, b2, s2, nameL2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof EmploeeWritable && b instanceof EmploeeWritable) {
                System.out.println("a).getName()=" + ((EmploeeWritable) a).getName() + "b).getName()=" + ((EmploeeWritable) b).getName());
                System.out.println("a).getRole()=" + ((EmploeeWritable) a).getRole() + "b).getRole()=" + ((EmploeeWritable) b).getRole());
                return -((EmploeeWritable) a).getName().compareTo(((EmploeeWritable) b).getName());
            }
            return super.compare(a, b);
        }
    }

    /***
     * 按词频降序排序
     * 的类
     *
     * **/
    public static class DescSort extends  WritableComparator{

        public DescSort() {
            super(IntWritable.class,true);//注册排序组件
        }
        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                           int arg4, int arg5) {
            return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);//注意使用负号来完成降序
        }

        @Override
        public int compare(Object a, Object b) {

            return   -super.compare(a, b);//注意使用负号来完成降序
        }

    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
