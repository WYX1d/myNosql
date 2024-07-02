
package client;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.Scanner;

public class CmdClient{
    private Client client;

    public CmdClient() {}

    public CmdClient(Client client) {
        this.client = client;
    }

    public void main(){
        Scanner scanner = new Scanner(System.in);
        String[] input = new String[1];
        while (true){
            input[0] =  scanner.nextLine();
            CMD(input);
        }
    }

    public void CMD(String[] input) {
        // 创建 Options 对象
        Options options = new Options();

        // 添加 -s 选项，它接受两个参数（key 和 value）
        Option setOption = Option.builder("s")
                .longOpt("set")
                .hasArg()
                .numberOfArgs(2)
                .argName("key value")
                .desc("设置键值")
                .build();
        options.addOption(setOption);

        // 添加 -g 选项，它接受一个参数（key）
        Option getOption = Option.builder("g")
                .longOpt("get")
                .hasArg()
                .argName("key")
                .desc("获取值")
                .build();
        options.addOption(getOption);

        // 添加 -rm 选项，它接受一个参数（key）
        Option rmOption = Option.builder("r")
                .longOpt("remove")
                .hasArg()
                .argName("key")
                .desc("删除键值")
                .build();
        options.addOption(rmOption);

        // 添加 -h 或 --help 选项
        Option helpOption = Option.builder("h")
                .longOpt("help")
                .desc("打印帮助信息")
                .build();
        options.addOption(helpOption);

        // 创建 HelpFormatter
        HelpFormatter formatter = new HelpFormatter();

        // 创建 DefaultParser 对象
        CommandLineParser parser = new DefaultParser();

        // 解析命令行参数
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, input);
        } catch (ParseException e) {
            System.err.println("Parsing failed.  Reason: " + e.getMessage());
            formatter.printHelp("wyxdb", options);
            System.exit(1);
        }

        System.out.println(Arrays.toString(cmd.getOptions()));

        // 进行条件判断
        if (cmd.hasOption("s")) {
            String[] setArgs = cmd.getOptionValue("s").split(" ");
            String key = setArgs[1];
            String value = setArgs[2];
            client.set(key, value);
        } else if (cmd.hasOption("g")) {
            String[] getArgs = cmd.getOptionValue("g").split(" ");
            String key = getArgs[1];
            client.get(key);
        } else if (cmd.hasOption("r")) {
            String[] removeArgs = cmd.getOptionValue("r").split(" ");
            String key = removeArgs[1];
            client.rm(key);
        } else if (cmd.hasOption("h")) {
            PrintHelp(formatter, options);
        } else {
            PrintHelp(formatter, options);
        }
    }

    // 打印帮助信息
    private static void PrintHelp(HelpFormatter formatter, Options options) {
        formatter.printHelp("wyxdb", options, true);
    }

    public void Test(String[] input) {

    }
}
