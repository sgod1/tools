package org.szesto;

import java.io.*;
import java.nio.file.Path;
import java.util.Optional;

public class InputWorker {
    public InputWorker() {
    }

    public static String readInputStream(InputStream inputStream) {
        StringBuilder sb = new StringBuilder();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        bufferedReader.lines().forEach(sb::append);
        return sb.toString();
    }

    public static String readStdin() {
        return readInputStream(System.in);
    }

    public static String readFile(String path) throws IOException {
        try (FileInputStream fis = new FileInputStream(absolutePath(path).toString())) {
            return readInputStream(fis);
        }
    }

    public static Path absolutePath(String path) {
        return Path.of(path).toAbsolutePath();
    }

    public static boolean fileExists(String path) {
        return absolutePath(path).toFile().exists();
    }

    public static boolean fileMissing(String path) {
        return ! fileExists(path);
    }

    public Optional<String> nextMessage() {
        return Optional.of("hello, world");
    }
}
