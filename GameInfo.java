public class GameInfo {
	int points;
	int actionCount;
	boolean[] specialMoves;
	String user;
	int id;
	int specialCount;

	public GameInfo(String user, int id) {
		points = 0;
		actionCount = 1;
		specialMoves = new boolean[4];
		this.user = user;
		this.id = id;
		specialCount = 0;
	}

	public int addPoints(int value) {
		return (points += value);
	}

	public int getPoints() {
		return points;
	}

	public int incrementCount() {
		return ++actionCount;
	}

	public int getCount() {
		return actionCount;
	}

	public void useSpecialMove(int move) {
		specialMoves[move] = true;
		specialCount++;
	}

	public boolean checkSpecialMove(int move) {
		return specialMoves[move];
	}

	public String getUser() {
		return user;
	}

	public int getId() {
		return id;
	}

	public boolean checkSpecial() {
		if(specialCount == 4) {
			return true;
		}
		else {
			return false;
		}
	}
}