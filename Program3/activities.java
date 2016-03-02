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

public class activities extends Configured implements Tool {

  public static class activitiesMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    private Text        outputKey   = new Text();
    private Text outputValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
     try { 
      JSONObject json = new JSONObject(value.toString());
      JSONObject actionJson = json.getJSONObject("action");
      String userId = json.getString("user");
      String action = actionJson.getString("actionType");
      if(action.equals("gameStart") || action.equals("gameEnd")) {
        outputKey.set(userId);
        outputValue.set(value.toString());
        context.write(outputKey, outputValue);
      }

    } catch (Exception e) {System.out.println(e); }
    }
  }

  public static class activitiesReducer
      extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      JSONObject json;
      JSONObject actionJson;
      boolean firstScore = true;
      String action;
      String outcome;
      int gamesPlayed = 0;
      int gamesWon = 0;
      int gamesLost = 0;
      int highScore = 0;
      int longestGame = 0;
      int score;
      int moves;
      for (Text val : values) {
        json = new JSONObject(val.toString());
        actionJson = json.getJSONObject("action");
        action = actionJson.getString("actionType");
        if(action.equals("gameEnd")) {
          outcome = actionJson.getString("gameStatus");
          if(outcome.equals("WON")) {
            gamesWon++;
          }
          else {
            gamesLost++;
          }
          score = actionJson.getInt("points");
          if(firstScore) {
            highScore = score;
            firstScore = false;
          }
          else {
            if(score > highScore) {
              highScore = score;
            }
          }
          moves = actionJson.getInt("actionNumber");
          if(moves > longestGame) {
            longestGame = moves;
          }
        }
        else if (action.equals("gameStart")){
          gamesPlayed++;
        }

      }

      json = new JSONObject();
      json.put("gamePlayed", gamesPlayed);
      json.put("gamesWon", gamesWon);
      json.put("gamesLost", gamesLost);
      json.put("highestGameEndScore", highScore);
      json.put("largestNMoves", longestGame);

      result.set(json.toString());
      context.write(key, result);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = super.getConf();
    Job job = Job.getInstance(conf, "activities job");

    job.setJarByClass(activities.class);
    job.setMapperClass(activitiesMapper.class);
    job.setReducerClass(activitiesReducer.class);
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
    int res = ToolRunner.run(conf, new activities(), args);
    System.exit(res);
  }
}
