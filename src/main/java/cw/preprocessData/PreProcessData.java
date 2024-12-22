package cw.preprocessData;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PreProcessData {
    public static void main(String[] args) {
        Path inputFilePath = Paths.get("src/main/resources/playerdataset.csv");
        String outputFilePath = "dataset.csv";

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath.toFile()));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {

            String header = br.readLine();
            System.out.println("HEADER");
            System.out.println(header);
            if (header != null) {
                bw.write(header + ",HOMESCORE,AWAYSCORE\n");
            }

            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",", -1);
                String score = columns[columns.length - 3];

                String homeScore = "";
                String awayScore = "";

                if (score != null && !score.isEmpty()) {
                    String[] scores = score.split("\\s*-\\s*");
                    if (scores.length == 2 && isNumeric(scores[0]) && isNumeric(scores[1])) {
                        homeScore = scores[0].trim();
                        awayScore = scores[1].trim();
                    }
                }

                bw.write(line + "," + homeScore + "," + awayScore + "\n");
            }

            System.out.println("Dataset preprocessing completed. Processed file saved at: " + outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
