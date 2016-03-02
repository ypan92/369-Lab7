import org.json.JSONObject;
import org.json.*;
import java.io.*;
import java.util.*;

public class histogramSeq {
	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println("Usage: input filename");
		}
		else {
			String filename = args[0];
			HashMap<String, Integer> locationCount = new HashMap<String, Integer>();
			try {
				final long startTime = System.currentTimeMillis();
				JSONTokener t = new JSONTokener(new FileReader(new File(filename)));
				JSONArray a = new JSONArray(t);
				JSONObject current;
				String key;
				int x;
				int y;
				for(int i = 0; i < a.length(); i++) {
					current = a.getJSONObject(i).getJSONObject("action");
					
					if(current.getString("actionType").equals("Move")) {
						current = current.getJSONObject("location");
						x = current.getInt("x");
						y = current.getInt("y");
						key = "(" + x + "," + y + ")";
						if(locationCount.containsKey(key)) {
							locationCount.put(key, locationCount.get(key)+1);
						}
						else {
							locationCount.put(key, 1);
						}
					}
				}

				final long endTime = System.currentTimeMillis();
				System.out.println("Total execution time: " + (double)(endTime - startTime) / 1000);

				/*for (Map.Entry<String,Integer> entry : locationCount.entrySet()) {
					key = entry.getKey();
					int value = entry.getValue();
					System.out.println(key + " " + value);
				}*/
			}
			catch(Exception e) {
				System.out.println("bad " + e);
			}
		}
	}
}