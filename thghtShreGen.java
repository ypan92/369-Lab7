import org.json.*;

import java.io.*;
import java.util.*;

class thghtShreGen {
    private static final int MAX_USERS = 10000;
    private static final int WORD_FILE_SIZE = 6832;
    private static ArrayList<String> words = new ArrayList<String>();

    private static String getStatus() {
        int dice = randNum(1, 100);
        String status;
        
        if (dice <= 60)
            status = "public";
        else if (dice <= 80)
            status = "protected";
        else
            status = "private";

        return status;
    }

    private static String getRecepient(String status) {
        int dice = randNum(1, 100);
        String recepient;
        String user = "u" + randNum(1, MAX_USERS);
        
        if (status.equals("public")) {
            if (dice <= 40)
                recepient = "all";
            else if (dice <= 80)
                recepient = "subscribers";
            else if (dice <= 90)
                recepient = user;
            else
                recepient = "self";
        } else if (status.equals("protected")) {
            if (dice <= 70)
                recepient = "subscribers";
            else if (dice <= 85)
                recepient = "self";
            else
                recepient = user;
        } else {
            if (dice <= 90)
                recepient = user;
            else
                recepient = "self";
        }

        return recepient;
    }

    // Inclusive
    private static int randNum(int min, int max) {
        return (int) (Math.random() * (max - min + 1)) + min;
    }

    private static boolean shouldRespond() {
        int dice = randNum(1, 100);

        return dice <= 20;
    }

    private static String getText() {
        int numWords = randNum(2, 20);
        String text = "";
        
        for (int i = 0; i < numWords; i++) {
            text += words.get(randNum(0, WORD_FILE_SIZE));
            if (i != numWords - 1)
                text += " ";
        }

        return text;
    }

    private static void scanWordFile() {
        Scanner scanner;

        try {
            scanner = new Scanner(new File("sense.txt"));

            while (scanner.hasNextLine()) {
                words.add(scanner.nextLine());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void generateJSON(String filename, int numObjects) {
        PrintWriter writer = null;
        JSONObject object;
        int messageId = 0;
        String status;

        scanWordFile();

        try {
            writer = new PrintWriter(new FileWriter(filename));
            writer.println("[");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < numObjects; i++) {
            status = getStatus();
            try {
                object = new JSONObject();
                object.put("messageId", messageId++);
                object.put("user", "u" + randNum(1, MAX_USERS));
                object.put("status", status);
                object.put("recepient", getRecepient(status));
                
                if (shouldRespond() && i > 0)
                    object.put("in-response", messageId - randNum(0, messageId));
                object.put("text", getText());

                writer.println(object.toString(2) + ",");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        writer.println("]");
        writer.close();
    }

    public static void main(String[] args) {
        try {
            generateJSON(args[0], Integer.parseInt(args[1]));
        }
        catch (NumberFormatException e) {
            System.out.println(args[1] + " not a number");
        }
        catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Not enough input parameters");
        }
    }
}
