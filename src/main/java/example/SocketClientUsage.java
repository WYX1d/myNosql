/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.CmdClient;
import client.SocketClient;

import java.util.Scanner;

public class SocketClientUsage {
    private static String host = "localhost";
    private static int port = 12345;
   private static Client client = new SocketClient(host, port);
    public static void main(String[] args) {
        CmdClient cmdClient = new CmdClient(client);
        cmdClient.main();
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
                } else {
                    System.out.println("Invalid command format: get <key>");
                }
                break;
            case "set":
                if (key != null && value != null) {
                    client.set(key, value);
                } else {
                    System.out.println("Invalid command format: set <key> <value>");
                }
                break;
            case "rm":
                if (key != null) {
                    client.rm(key);
                } else {
                    System.out.println("Invalid command format: rm <key>");
                }
                break;
            default:
                System.out.println("Unsupported command: " + command);
        }
    }
}