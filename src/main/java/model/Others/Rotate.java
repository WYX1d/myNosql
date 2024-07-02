package model.Others;

import lombok.Getter;
import model.command.CommandPos;
import service.NormalStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
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
        ClearDataBaseFile("my_db");

        // 压缩日志文件
        CompressIndexFile(this.genFilePath);


    }

    public String toString() {
        return "Rotate{}";
    }
    //压缩文件操作
    public void CompressIndexFile(String filePath) {
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
            //arrayList.remove(arrayList.size() - 1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        ClearDataBaseFile(this.genFilePath);

        try (FileWriter writer = new FileWriter(this.genFilePath)) {
            for (String line : arrayList) {
                writer.write(line + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void ClearDataBaseFile(String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            // 无法写入
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
