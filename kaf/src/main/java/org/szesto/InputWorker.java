package org.szesto;

import java.io.*;
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
        try (FileInputStream fis = new FileInputStream(path)) {
            return readInputStream(fis);
        }
    }

    public Optional<String> nextMessage() {
        return Optional.of("hello, world");
    }
}
