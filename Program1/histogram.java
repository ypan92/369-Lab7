import java.io.IOException;
import java.util.Iterator;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;

public class histogram extends Configured implements Tool {

  public static class HistogramMapper
      extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text        outputKey   = new Text();
    private IntWritable outputValue = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
     try { 
      JSONObject json = new JSONObject(value.toString());
      JSONObject location = json.getJSONObject("action");
      if(location.getString("actionType").equals("Move")) {
        location = location.getJSONObject("location");
        int x = location.getInt("x");
        int y = location.getInt("y");
        String coords = "(" + x + "," + y + ")";

        outputKey.set(coords);
        context.write(outputKey, outputValue);
      }

    } catch (Exception e) {System.out.println(e); }
    }
  }

  public static class HistogramReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = super.getConf();
    Job job = Job.getInstance(conf, "histogram job");

    job.setJarByClass(histogram.class);
    job.setMapperClass(HistogramMapper.class);
    job.setReducerClass(HistogramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(MultiLineJsonInputFormat.class);
    MultiLineJsonInputFormat.setInputJsonMember(job, "game");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new histogram(), args);
    System.exit(res);
  }
}
