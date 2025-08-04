package dirwalker;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DirectoryContentMerger {
    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.out.println("Usage: java DirectoryContentMerger <input_directory> <output_file>");
//            System.exit(1);
//        }
//
//        String inputDirectory = args[0];
//        String outputFile = args[1];

        String inputDirectory = "C:\\git_m\\kafka-consumer-service\\src";
        String outputFile = "C:\\git_m\\content_merger\\f1.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            processDirectory(Paths.get(inputDirectory), writer);
            System.out.println("All files content has been merged into " + outputFile);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static void processDirectory(Path directory, BufferedWriter writer) throws IOException {
        File[] files = directory.toFile().listFiles();

        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                processDirectory(file.toPath(), writer);
            } else {
                if (file.isFile() && file.canRead()) {
                    writeFileContent(file, writer);
                }
            }
        }
    }

    private static void writeFileContent(File file, BufferedWriter writer) throws IOException {
        writer.write("=== File: " + file.getPath() + " ===\n");
        writer.write("Size: " + file.length() + " bytes\n");
        writer.write("Content:\n");

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line + "\n");
            }
        }

        writer.write("========================================================================================\n"); // Добавляем пустую строку между файлами
    }
}



