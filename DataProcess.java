import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.HashSet;

public class DataProcess extends Configured implements Tool {
    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> label = new HashMap<String, String>();
        private HashMap<String, String> data = new HashMap<String, String>();
        private List<String> buffer = new LinkedList<String>();

        private boolean skip = false;

        private final int MAX_BLOCK_LINE = 50;

        public void setup(Context context) {
            Path labelPath = null;
            Path dataPath = null;
            String line;
            String[] tokens;
            String[] tokens1;
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                        .getConfiguration());
                if (null != cacheFiles && cacheFiles.length == 2) {
                    if (cacheFiles[0].toString().contains("bad_disk_list")) {
                        labelPath = cacheFiles[0];
                        dataPath = cacheFiles[1];
                    } else {
                        labelPath = cacheFiles[1];
                        dataPath = cacheFiles[0];
                    }

                    BufferedReader brData = new BufferedReader(new FileReader(
                            dataPath.toString()));
                    BufferedReader brLabel = new BufferedReader(new FileReader(
                            labelPath.toString()));
                    try {
                        while ((line = brData.readLine()) != null) {
                            tokens = line.split("\t", 2);
                            data.put(tokens[0], tokens[1]);
                        }
                        while ((line = brLabel.readLine()) != null) {
                            tokens1 = line.split("\t", 3);
                            label.put((tokens1[1] + tokens1[2]).trim(), "-1");
                        }
                        System.out.println("data size = " + data.size());
                        System.out.println("label size = " + label.size());
                    } finally {
                        brData.close();
                        brLabel.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String splitArr[] = line.split("\\s+");
            if (splitArr == null) {
                return;
            }
            if (splitArr.length == 1 || skip) {
                func(buffer, context);
                buffer.removeAll(buffer);
                if (splitArr.length == 1) {
                    skip = false;
                }
            } else {
                if (skip) {
                    return;
                }
                buffer.add(line);
                if (buffer.size() >= MAX_BLOCK_LINE) {
                    skip = true;
                }
            }
        }

        public void func(List<String> list,
                         org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            // do you own logic
            //
            Iterator itr = list.iterator();
            if (list.size() == 0){
                return;
            }

            String hostDisk=null;
            StringBuffer rst = new StringBuffer();
            String Time[] = null;
            int i = 1;
            while (itr.hasNext()) {
                String line = (String)itr.next();
                String splitArr[] = line.split("\\s+");
                if (splitArr.length < 13){
                    continue;
                }
                try{
                    Integer.parseInt(splitArr[6]);
                }catch(NumberFormatException e){
                    continue;
                }

                hostDisk = splitArr[0] + splitArr[1];
                Time = splitArr[2].split(":");
                rst.append(String.valueOf(i)+":"+String.valueOf(splitArr[6])+"\t"+String.valueOf(splitArr[7])+"\t"+String.valueOf(splitArr[8])+"\t"+String.valueOf(splitArr[12])+"\t");
                i++;
            }
            // write out the result
            try{
                Text newKey= new Text(hostDisk+"\t"+Time[0]);
                Text newValue = new Text(rst.toString());
                context.write(newKey, newValue);
            }catch(NullPointerException e){
                return;
            }

        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for(Text val:values){
                context.write(key, val);
                return;
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage:hadoop jar DataUnification.jar DataUnification <input> <output> ");
            System.exit(-1);
        }
        Configuration conf = getConf();
        DistributedCache.addCacheFile(new URI("hdfs://w-namenode1v.cldiskb.shht.qihoo.net:9000/user/xitong/test/distributedcache/bad_disk_list#bad_disk_list"), conf);
        DistributedCache.addCacheFile(new URI("hdfs://w-namenode1v.cldiskb.shht.qihoo.net:9000/user/xitong/test/distributedcache/data#data"), conf);
        DistributedCache.createSymlink(conf);
        Job job = new Job(conf);
        job.setJarByClass(DataProcess.class);
        job.setJobName("DataProcess");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(100);
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new DataProcess(), args);
        System.exit(ret);
    }
}