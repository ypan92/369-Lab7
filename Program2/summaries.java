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

public class summaries extends Configured implements Tool {

  public static class summariesMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    private Text        outputKey   = new Text();
    private Text outputValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
     try { 
      JSONObject json = new JSONObject(value.toString());
      String userId = json.getString("user");
      outputKey.set(userId);
      outputValue.set(value.toString());
      context.write(outputKey, outputValue);

    } catch (Exception e) {System.out.println(e); }
    }
  }

  public static class summariesReducer
      extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      JSONObject json;
      JSONObject actionJson;
      int numMoves = 0;
      int regMoves = 0;
      int specMoves = 0;
      String outcome;
      int finalScore = 0;
      int avgPoints;
      String action;
      for (Text val : values) {
        json = new JSONObject(val.toString());
        actionJson = json.getJSONObject("action");
        action = actionJson.getString("actionType");
        if(action.equals("gameEnd")) {
          outcome = actionJson.getString("gameStatus");
        }
        //Move was made
        else if (!action.equals("gameStart")){
          numMoves++;
          finalScore += actionJson.getInt("pointsAdded");
          if(action.equals("Move")) {
            regMoves++;
          }
          //special move
          else {
            specMoves++;
          }

        }

      }
      avgPoints = finalScore / numMoves;
      json = new JSONObject();
      json.put("user", key.toString());
      json.put("moves", numMoves);
      json.put("regular", regMoves);
      json.put("special", specMoves);
      json.put("score", finalScore);
      json.put("perMove", avgPoints);
      result.set(json.toString());
      context.write(key, result);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = super.getConf();
    Job job = Job.getInstance(conf, "summaries job");

    job.setJarByClass(summaries.class);
    job.setMapperClass(summariesMapper.class);
    job.setReducerClass(summariesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(MultiLineJsonInputFormat.class);
    MultiLineJsonInputFormat.setInputJsonMember(job, "game");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new summaries(), args);
    System.exit(res);
  }
}
