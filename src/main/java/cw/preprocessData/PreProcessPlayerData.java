package cw.preprocessData;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.*;

public class PreProcessPlayerData {

    public static void main(String[] args) throws IOException {
        Path inputFilePath = Paths.get("src/main/resources/dataset.csv");
        String outputFilePath = "playerdataset.csv";


        // Set up input and output streams
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath.toFile()));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));

        // Write the header for the output file
        writer.write("PlayerName,Points\n");

        // Regular expression to extract player name and score
        Pattern scorePattern = Pattern.compile("(\\d+) PTS");

        // Read and process each line
        String line;
        while ((line = reader.readLine()) != null) {
            String[] columns = line.split(",", -1); // Handle potential missing columns
            if (columns.length > 28) { // Ensure sufficient columns are present
                String homeDescription = columns[3]; // HOMEDESCRIPTION
                String visitorDescription = columns[26]; // VISITORDESCRIPTION

                // Extract data for PLAYER1_NAME, PLAYER2_NAME, and PLAYER3_NAME
                String player1Name = columns[7];
                String player2Name = columns[13];
                String player3Name = columns[19];

                // Process descriptions and write results
                if (homeDescription != null) {
                    processDescription(homeDescription, player1Name, scorePattern, writer);
                    processDescription(homeDescription, player2Name, scorePattern, writer);
                    processDescription(homeDescription, player3Name, scorePattern, writer);
                }

                if (visitorDescription != null) {
                    processDescription(visitorDescription, player1Name, scorePattern, writer);
                    processDescription(visitorDescription, player2Name, scorePattern, writer);
                    processDescription(visitorDescription, player3Name, scorePattern, writer);
                }

            }
        }

        // Close streams
        reader.close();
        writer.close();
    }

    private static void processDescription(String description, String playerName, Pattern scorePattern, BufferedWriter writer) throws IOException {
        // Validate playerName to avoid empty or null names
        if (playerName != null && !playerName.trim().isEmpty()) {
            // First, check if the description contains "(x PTS)" using the regex
            String formattedDescription = getDescription(description);
            Matcher matcher = scorePattern.matcher(formattedDescription);
            if (matcher.find()) {
                // Extract the score from the regex match
                int score = Integer.parseInt(matcher.group(1));

                // Split player's name into first and last names for partial matching
                String[] nameParts = playerName.split(" ");
                boolean nameMatch = false;

                // Check if any part of the player's name exists in the description
                for (String part : nameParts) {
                    if (formattedDescription.contains(part)) {
                        nameMatch = true;
                        break;
                    }
                }

                // Write output only if a valid player name is matched
                if (nameMatch) {
                    writer.write(playerName + "," + score + "\n");
                }
            }
        }
    }


    public static String getDescription(String description) {
        // Split the string on "PTS)" and include the delimiter for proper matching
        String delimiter = "PTS)";
        int index = description.indexOf(delimiter);

        // If "PTS)" exists in the string, return the part before it plus the delimiter
        if (index != -1) {
            return description.substring(0, index + delimiter.length());
        } else {
            return "";
        }
    }



}
