/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.SocketClient;

import java.util.Scanner;

public class SocketClientUsage {
    private static SocketClient client;
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
//        client.get("zsy1");
//        client.set("zsy12","for test");
//        client.set("wyx1","1");
//        client.set("wyx2","2");
//        client.set("wyx3","3");
//        client.set("wyx4","4");
//        client.set("wyx5","5");
//        client.set("wyx6","6");
//        client.get("zsy12");
//        client.get("wyx2");
//        client.get("wyx4");
//        client.set("wyx4","4.1");
//        client.rm("zsy12");
//        client.rm("wyx4");
        client.rm("wyx6");
//        client.get("zsy12");
        // 开始用户交互
//        startUserInteraction();
    }
    private static void startUserInteraction() {
        Scanner scanner = new Scanner(System.in);
        String input;

        while (true) {
            System.out.println("Enter command (get/set/rm) or 'exit' to quit:");
            input = scanner.nextLine().trim();

            if ("exit".equalsIgnoreCase(input)) {
                break;
            }

            processUserCommand(input);
        }

        scanner.close();
        System.out.println("Exiting the client.");
    }

    private static void processUserCommand(String input) {
        String[] parts = input.split("\\s+", 3);
        if (parts.length == 0) {
            return;
        }

        String command = parts[0];
        String key = parts.length > 1 ? parts[1] : null;
        String value = parts.length > 2 ? parts[2] : null;

        switch (command.toLowerCase()) {
            case "get":
                if (key != null) {
                    String result = client.get(key);
                    System.out.println("GET " + key + " -> " + result);
                } else {
                    System.out.println("Invalid command format: get <key>");
                }
                break;
            case "set":
                if (key != null && value != null) {
                    client.set(key, value);
                    System.out.println("SET " + key + " -> " + value);
                } else {
                    System.out.println("Invalid command format: set <key> <value>");
                }
                break;
            case "rm":
                if (key != null) {
                    client.rm(key);
                    System.out.println("RM " + key);
                } else {
                    System.out.println("Invalid command format: rm <key>");
                }
                break;
            default:
                System.out.println("Unsupported command: " + command);
        }
    }
}