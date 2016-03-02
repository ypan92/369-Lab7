import org.json.*;
import java.util.*;
import java.io.*;

public class BeFuddledGen {

	private final int max_users = 10000;
	private Hashtable<Integer, GameInfo> gameRecord;
	private boolean[] userIds;
	private final int SHUFFLE = 0;
	private final int CLEAR = 1;
	private final int INVERT = 2;
	private final int ROTATE = 3;


    public static void main(String[] args) {

    	String outputFilename;
    	int size = 0;
       	int jsonCount = 0;
       	File outputFile;
       	BeFuddledGen game = new BeFuddledGen();
    	Scanner scan = new Scanner(System.in);
    	System.out.println("Input output file name: ");
    	outputFilename = scan.next();
    	System.out.println("Input number of JSON to generate: ");
    	size = scan.nextInt();
    	scan.close();
    	game.createData(size, outputFilename);
	}

	private void createData(int size, String outputFilename) {
		int command;
		int userId;
		int gameCount = 1;
		Random randomNum = new Random();
		File outputFile = new File(outputFilename);
		PrintWriter writer = null;
		JSONObject jObj = null;
		userIds = new boolean[max_users + 1];
		gameRecord = new Hashtable<Integer, GameInfo>();

		try {
			writer = new PrintWriter(new FileWriter(outputFile));
		}
		catch (IOException e) {
			System.out.println("Could not create file " + outputFilename);
		}

		for(int i = 0; i < size; i++) {
			if(gameRecord.isEmpty()) {
				jObj = createNewUser(gameCount++);
			}
			else if(gameRecord.size() == max_users) {
				jObj = endGame(gameCount);
			}
			else {
				command = randomNum.nextInt(14);
				switch(command) {
					case 0:
						jObj = createNewUser(gameCount++);
						break;
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
					case 8:
						jObj = moveUser(gameCount);
						break;
					case 9:
					case 10:
					case 11:
					case 12:
						jObj = specialMove(gameCount);
						break;
					case 13:
						jObj = endGame(gameCount);
						break;
				}
			}
			printObj(jObj, writer);
			writer.write("\n");
		}
		writer.close();
	}

	private JSONObject createNewUser(int gameId) {
		int userId;
		JSONObject jObj = new JSONObject();
		JSONObject action = new JSONObject();
		Random randomNum = new Random();
		userId = randomNum.nextInt(max_users) + 1;
		while(userIds[userId] == true) {
			userId = randomNum.nextInt(max_users) + 1;
		}
		userIds[userId] = true;
		try {
			jObj.put("game", gameId);
			action.put("actionType", "gameStart");
			action.put("actionNumber", 1);
			jObj.put("action", action);
			jObj.put("user", "u" + userId);
		}
		catch (JSONException e) {
			System.out.println("could not put");
		}
		gameRecord.put(gameId, new GameInfo("u" + userId, userId));
		return jObj;
	}

	private JSONObject moveUser(int gameCount) {
		Random randomNum = new Random();
		GameInfo gameInfo;
		JSONObject jObj = new JSONObject();
		JSONObject action = new JSONObject();
		JSONObject coords = new JSONObject();
		int game;
		int xCoord;
		int yCoord;
		int addPoints;
		//Gets random number to denote which game to use action on
		game = getRandomGame(gameCount);

		xCoord = randomNum.nextInt(20) + 1;
		yCoord = randomNum.nextInt(20) + 1;
		addPoints = randomNum.nextInt(41) - 20;
		gameInfo = gameRecord.get(game);
		gameInfo.addPoints(addPoints);
		gameInfo.incrementCount();
		try {
			jObj.put("game", game);

			coords.put("x", xCoord);
			coords.put("y", yCoord);
			
			action.put("actionType", "Move");
			action.put("actionNumber", gameInfo.getCount());
			action.put("location", coords);
			action.put("pointsAdded", addPoints);
			action.put("points", gameInfo.getPoints());

			jObj.put("action", action);
			jObj.put("user", gameInfo.getUser());
		}
		catch (JSONException e) {
			System.out.println("could not put");
		}
		return jObj;
	}

	private JSONObject specialMove(int gameCount) {
		Random randomNum = new Random();
		GameInfo gameInfo;
		JSONObject jObj = new JSONObject();
		JSONObject action = new JSONObject();
		int game;
		int addPoints;
		int special;
		//Gets random number to denote which game to use action on
		game = getRandomGame(gameCount);
		gameInfo = gameRecord.get(game);
		//Checks if all specials are used
		if(gameInfo.checkSpecial()) {
			return moveUser(gameCount);
		}
		//Gets random number to denote which special move to use
		special = randomNum.nextInt(4);
		while(gameInfo.checkSpecialMove(special)) {
			special = randomNum.nextInt(4);
		}
		gameInfo.useSpecialMove(special);

		addPoints = randomNum.nextInt(41) - 20;
		gameInfo.addPoints(addPoints);
		gameInfo.incrementCount();

		try {
			jObj.put("game", game);

			action.put("actionType", "specialMove");
			action.put("actionNumber", gameInfo.getCount());
			action.put("pointsAdded", addPoints);
			if(special == SHUFFLE) {
				action.put("move", "Shuffle");
			}
			else if(special == CLEAR) {
				action.put("move", "Clear");
			}
			else if(special == INVERT) {
				action.put("move", "Invert");
			}
			else {
				action.put("move", "Rotate");
			}
			action.put("points", gameInfo.getPoints());

			jObj.put("action", action);
			jObj.put("user", gameInfo.getUser());
		}
		catch (JSONException e) {
			System.out.println("could not put");
		}
		return jObj;	
	}

	private JSONObject endGame(int gameCount) {
		Random randomNum = new Random();
		GameInfo gameInfo;
		JSONObject jObj = new JSONObject();
		JSONObject action = new JSONObject();
		int game;
		String status;

		game = getRandomGame(gameCount);
		gameInfo = gameRecord.get(game);
		gameInfo.incrementCount();
		if(gameInfo.getCount() < 9) {
			return moveUser(gameCount);
		}
		if(gameInfo.getPoints() > 40 || gameInfo.getPoints() < -40) {
			status = "WIN";
		}
		else {
			status = "LOSS";
		}
		try {
			jObj.put("game", game);

			action.put("actionType", "gameEnd");
			action.put("gameStatus", status);
			action.put("actionNumber", gameInfo.getCount());
			action.put("points", gameInfo.getPoints());

			jObj.put("action", action);
			jObj.put("user", gameInfo.getUser());
			userIds[gameInfo.getId()] = false;
			gameRecord.remove(game);
		}
		catch (JSONException e) {
			System.out.println("could not put");
		}
		return jObj;

	}

	private int getRandomGame(int num) {
		int ranGame;
		Random randomNum = new Random();
		ranGame = randomNum.nextInt(num) + 1;
		while(!gameRecord.containsKey(ranGame)) {
			ranGame = randomNum.nextInt(num) + 1;
		}
		return ranGame;
	}

	private void printObj(JSONObject jObj, PrintWriter writer) {
		try {
			//System.out.println(jObj.toString(3));
			writer.print(jObj.toString(3));
		}
		catch (JSONException e) {
			System.out.println("could to to string json object");
		}

	}
}

