import java.io.IOException;
import java.util.Iterator;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
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


public class accounting extends Configured implements Tool {

	public static class AccountingMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				JSONObject json = new JSONObject(value.toString());
				String text = json.getString("text");
				String user = json.getString("user");

				double cost = 0.05;
				int byteChargeCount = text.length() / 10;
				if (text.length() % 10 > 0) {
					byteChargeCount++;
				}
				for (int charge = 0; charge < byteChargeCount; charge++) {
					cost += 0.01;
				}
				if (text.length() > 100) {
					cost += 0.05;
				}

				String costStr = "" + cost;

				outputKey.set(user);
				outputValue.set(costStr);
				context.write(outputKey, outputValue);
			}
			catch (Exception e) {
				System.out.println(e);
			}
		}

	}

	public static class AccountingReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		//@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			int msgCount = 0;
			double cost = 0.0;
			for (Text val : values) {
				msgCount++;
				cost += Double.parseDouble(val.toString());
			}

			if (msgCount > 100) {
				cost *= .95;
			}

			DecimalFormat df = new DecimalFormat("$#.##");
			String res = "" + df.format(cost);
			result.set(res);
			context.write(key, result);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "accounting job");

		job.setJarByClass(accounting.class);
		job.setMapperClass(AccountingMapper.class);
		job.setReducerClass(AccountingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(MultiLineJsonInputFormat.class);
		MultiLineJsonInputFormat.setInputJsonMember(job, "text");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int result = ToolRunner.run(conf, new accounting(), args);
		System.exit(result);
	}

}