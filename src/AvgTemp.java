package pds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgTemp {

    
    public static class AvgTempTuple implements Writable {

        private int avg = 0;
        private long count = 0;
        
        public AvgTempTuple() {}
        
        public AvgTempTuple(int temperature) {
            this.avg = temperature;
            this.count = 1;
        }
        
        public int getAvg() { return avg; }
        public long getCount() { return count; }
        public void setAvg(int avg) { this.avg = avg; }
        public void setCount(long count) { this.count = count; }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            avg = in.readInt();
            count = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(avg);
            out.writeLong(count);
        }
        
        public String toString() {
            return avg + ", " + count;
        }
    }
    
    public static class TupleMapper extends Mapper<Object, Text, Text, AvgTempTuple> {
        
        private AvgTempTuple outTuple = new AvgTempTuple();
        private Text month = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            month.set(line[1].substring(4,6));
            int temperature = Integer.parseInt(line[3]);
            
            outTuple.setAvg(temperature);
            outTuple.setCount(1);
            
            context.write(month, outTuple);
        }
        
    }
    
    public static class TupleReducer extends Reducer<Text, AvgTempTuple, Text, AvgTempTuple> {
        
        private AvgTempTuple result = new AvgTempTuple();
        
        public void reduce(Text key, Iterable<AvgTempTuple> values, Context context) 
                throws IOException, InterruptedException {
            
            result.setCount(0);
            result.setAvg(0);
            int sum = 0;
            int total = 0;
            for (AvgTempTuple value : values) {
                total += value.getAvg();
                sum += value.getCount();
            }
            result.setAvg(total/sum);
            result.setCount(sum);
            context.write(key,  result);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avg temp");
        job.setJarByClass(AvgTemp.class);
        job.setMapperClass(TupleMapper.class);
        job.setReducerClass(TupleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgTempTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
