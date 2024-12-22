package cw.preprocessData;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class GameWithPlayerData {

    public static void main(String[] args) {
        String inputFilePath = "src/main/resources/dataset.csv";
        String outputFilePath = "processed_dataset_with_player_scores.csv";
        Map<String, String> homeMap = new HashMap<>();
        Map<String, String> awayMap = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            String header = reader.readLine(); // Read the header
            if (header == null) {
                System.out.println("Empty file!");
                return;
            }

            // Write new header
            writer.write("GameID,Period,HomeTeam,AwayTeam,HomeScore,AwayScore,PlayerName,Points\n");

            String line;
            Pattern scorePattern = Pattern.compile("(\\d+) PTS");

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",", -1);

                String gameId = columns[2];
                String period = columns[5];
                String homeDescription = columns[3];
                String awayDescription = columns[26];
                String homeScore = columns[27];
                String awayScore = columns[28];
                String player1Name = columns[7];
                String player2Name = columns[13];
                String player3Name = columns[19];
                String player1Team = columns[11];
                String player2Team = columns[17];
                String player3Team = columns[23];
                String[] players = new String[]{player1Name, player2Name, player3Name};

                if (homeScore.isEmpty() && awayScore.isEmpty()) {
                    continue;
                }

                Map<String, String> playerMap = new HashMap<>();
                playerMap.put(player1Name, player1Team);
                playerMap.put(player2Name, player2Team);
                playerMap.put(player3Name, player3Team);

                String homeTeam = "";
                String awayTeam = "";
                boolean homeFound = false;
                boolean awayFound = false;

                for (String player : players) {
                    if (homeMap.containsKey(gameId) && awayMap.containsKey(gameId)) {
                        // Skip the loop if both maps already contain the gameId
                        break;
                    }

                    if (!player.isEmpty()) {
                        if (!homeDescription.isEmpty() && !homeFound) {
                            homeTeam = getTeam(homeDescription, player, playerMap);
                            homeFound = true;
                            addMatchToMap(homeMap, gameId, homeTeam);
                        } else if (!awayDescription.isEmpty() && !awayFound) {
                            awayTeam = getTeam(awayDescription, player, playerMap);
                            awayFound = true;
                            addMatchToMap(awayMap, gameId, awayTeam);
                        }
                    }
                }
//                writer.write(String.format("%s,%s,%s,%s,%s,%s\n", gameId, period, homeMap.getOrDefault(gameId, ""), awayMap.getOrDefault(gameId, ""), homeScore, awayScore));
                // Process player scores from descriptions
                processDescription(gameId, period, homeMap.getOrDefault(gameId, ""), awayMap.getOrDefault(gameId, ""), homeScore, awayScore, homeDescription, players, scorePattern, writer);
                processDescription(gameId, period, homeMap.getOrDefault(gameId, ""), awayMap.getOrDefault(gameId, ""), homeScore, awayScore, awayDescription, players, scorePattern, writer);
            }

            System.out.println("Data preprocessing complete. Processed data saved to: " + outputFilePath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addMatchToMap(Map<String, String> map, String gameId, String team) {
        if (!map.containsKey(gameId) && !team.isEmpty()) {
            map.put(gameId, team);
        }
    }

    private static String getTeam(String description, String playerName, Map<String, String> playerMap) {
        String playerTeam = "";

        if (!description.isEmpty()) {
            String[] nameParts = playerName.split(" ");
            boolean nameMatch = false;

            for (String part : nameParts) {
                if (description.contains(part)) {
                    nameMatch = true;
                    playerTeam = playerMap.get(playerName);
                    break;
                }
            }

        }
        return playerTeam;
    }

    private static void processDescription(String gameId, String period, String homeTeam, String awayTeam, String homeScore, String awayScore, String description, String[] players, Pattern scorePattern, BufferedWriter writer) throws IOException {
        if (description != null && !description.isEmpty()) {
            Matcher matcher = scorePattern.matcher(description);

            while (matcher.find()) {
                int score = Integer.parseInt(matcher.group(1));

                for (String player : players) {
                    if (player != null && !player.trim().isEmpty()) {
                        String[] nameParts = player.split(" ");
                        boolean nameMatch = false;

                        // Check if any part of the player's name exists in the description
                        for (String part : nameParts) {
                            if (description.contains(part)) {
                                nameMatch = true;
                                break;
                            }
                        }

                        if (nameMatch) {
                            writer.write(String.format("%s,%s,%s,%s,%s,%s,%s,%d\n", gameId, period, homeTeam, awayTeam, homeScore, awayScore, player, score));
                        }
                    }
                }
            }
        }
    }

}
