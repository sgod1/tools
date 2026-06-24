package org.szesto;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    public Optional<String> nextMessage() {
        return Optional.of("hello, world");
    }
}
