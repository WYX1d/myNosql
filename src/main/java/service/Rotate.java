package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import utils.CommandUtil;
import utils.LoggerUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Rotate extends Thread {
    @Getter
    private  String genFilePath;

    public Rotate(String filePath) {
        this.genFilePath = filePath;
    }

    @Override
    public void run() {
        // 压缩日志文件
        CompressIndexFile();

    }

    public String toString() {
        return "Rotate{}";
    }

    //压缩文件操作
    public boolean CompressIndexFile() {
        System.out.println("正在进行压缩...");
        ArrayList<String> arrayList = new ArrayList<>();
        ArrayList<String> arr = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(this.genFilePath))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                arrayList.add(line);//每行字符串添加到arrayList
                String key=tokey(line);
                arr.add(key);//每行字符串的key添加到arr
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        HashMap<String, Integer> lastIndexMap = new HashMap<>();
        for (int i = 0; i < arr.size(); i++) {
            String element = arr.get(i);
            lastIndexMap.put(element, i); // 更新元素最后出现的索引
        }

        //清空原文件
        ClearDataBaseFile();
        //重新写入
        try (FileWriter writer = new FileWriter(this.genFilePath)) {
            for (int i=0;i<arrayList.size();i++){
                if (lastIndexMap.containsValue(i)) {
                    //将lastIndexMap已经去重的元素顺序压缩保存
                    writer.write(arrayList.get(i) + "\r\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (lastIndexMap.size() < arrayList.size()) {
            System.out.println("数据已压缩");
            return true;
        } else {
            System.out.println("当前数据不可压缩");
            return false;
    }
    }
    //将每行字符串的key提取出来
    public String tokey(String line){
        String subItem = line.substring(4);
        System.out.println(subItem);
        JSONObject value = JSON.parseObject(new String(subItem));
        Command command = CommandUtil.jsonToCommand(value);
        return command.getKey();
    }

    public void ClearDataBaseFile() {
        try (FileWriter writer = new FileWriter(this.genFilePath)) {
            // 无法写入
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
