import java.io.IOException;
import java.util.*;

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


public class hashtags extends Configured implements Tool {

	public static class HashtagsMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private List<String> stopWords = Arrays.asList("a", "the", "in", "on", "I", "he", "she", "it", "there", "is");

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				JSONObject json = new JSONObject(value.toString());
				String text = json.getString("text");
				String user = json.getString("user");

				String[] words = text.split(" ");
				for (String word : words) {
					word = word.replaceAll("[^a-zA-Z0-9]", "");
					if (!stopWords.contains(word) && !word.equals("")) {
						outputKey.set(user);
						outputValue.set(word.toLowerCase());
						context.write(outputKey, outputValue);
					}
				}
			}
			catch (Exception e) {
				System.out.println(e);
			}
		}

	}

	public static class HashtagsReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
			int maxCount = 0;
			String hashtags = "";

			for (Text val : values) {
				String word = val.toString();
				if (wordCount.containsKey(word)) {
					int count = wordCount.get(word);
					count += 1;
					wordCount.put(word, count);
					if (count > maxCount) {
						maxCount = count;
					}
				}
				else {
					wordCount.put(word, 1);
					if (maxCount == 0) {
						maxCount = 1;
					}
				}
			}

			boolean hasMax = false;
			for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
				if (entry.getValue() == maxCount) {
					hashtags += entry.getKey() + ", ";
					hasMax = true;
				}
			}
			if (hasMax) {
				hashtags = hashtags.substring(0, hashtags.length() - 2);
			}

			result.set(hashtags);
			context.write(key, result);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "hashtags job");

		job.setJarByClass(hashtags.class);
		job.setMapperClass(HashtagsMapper.class);
		job.setReducerClass(HashtagsReducer.class);
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
		int result = ToolRunner.run(conf, new hashtags(), args);
		System.exit(result);
	}

}