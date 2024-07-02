package service;

import lombok.Getter;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

public class Rotate extends Thread {
    @Getter
    private static String genFilePath;

    public Rotate(String filePath) {
       this.genFilePath=filePath;
    }


    @Override
    public void run() {
        // 清空数据库文件
//        ClearDataBaseFile();

        // 压缩日志文件
        CompressIndexFile();


    }

    public String toString() {
        return "Rotate{}";
    }
    //压缩文件操作
    public void CompressIndexFile() {
        ArrayList<String> arrayList = new ArrayList<>();
        HashSet<String> hashSet = new HashSet<>();

        try (Scanner scanner = new Scanner(new File(this.genFilePath))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                // 处理每一行
                if (!hashSet.contains(line)) {
                    arrayList.add(line);
                    hashSet.add(line);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ClearDataBaseFile();
        try (FileWriter writer = new FileWriter(this.genFilePath)) {
            for (String line : arrayList) {
                writer.write(line + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void ClearDataBaseFile() {
        try (FileWriter writer = new FileWriter(this.genFilePath)) {
            // 无法写入
        } catch (IOException e) {
            e.printStackTrace();
        }
//        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
//            // 将文件指针移动到文件开头
//            raf.setLength(0); // 清空文件内容
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
