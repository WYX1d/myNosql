/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 *
 *
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
//import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author wyx
 */
public class NormalStore implements Store {

    public static final String TABLE = ".txt";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";


//    /**
//     * 内存表，类似缓存
//     */
//    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
    @Getter
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;


    /**
     * 持久化阈值
     */
    private static final int THRESHOLD_FOR_PERSISTENCE = 4;
    /*
    记录操作个数(set/rm)
    * */
    private int operateNums =0;

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
//        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndex();
    }

    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }



    public void reloadIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
//                     如果日志中记载该键值是被删除的，就将其从内存里删去
//                    if (command.getClass().equals(RmCommand.class)) {
//                        index.remove(command.getKey(), cmdPos);
//                    }//去掉删除的索引
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }

     //增改操作
    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = command.toByte();
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            if (operateNums >= THRESHOLD_FOR_PERSISTENCE) {
                operateNums = 0;
                System.out.println("开始压缩...");
                zipFile();
                System.out.println("数据已压缩到磁盘！");
            }
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);//返回偏移量

            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            operateNums++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }
    //将数据重新写入磁盘
    public void zipFile() {
        //rotate压缩文件
        Rotate rotate = new Rotate(this.genFilePath());
        rotate.start();
        //压缩索引
        index.clear();
        reloadIndex();
//        // 重写数据库文件
//        try (FileWriter writer = new FileWriter(this.genFilePath())) {
//            for (String key : this.getIndex().keySet()) {
//                writer.write(key + ":" + this.get(key) + "\r\n");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }


    //查询数据
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());

            JSONObject value = null;
            if (commandBytes != null) {
                value = JSONObject.parseObject(new String(commandBytes));
            }
            Command cmd = null;
            if (value != null) {
                cmd = CommandUtil.jsonToCommand(value);
            }
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }
    //删除操作
    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = command.toByte();
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            if (operateNums >= THRESHOLD_FOR_PERSISTENCE) {
                operateNums = 0;
                System.out.println("开始压缩...");
                zipFile();
                System.out.println("数据已压缩到磁盘！");
            }
            // 写table（wal）文件，返回偏移量
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
//            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            RandomAccessFileUtil.write(this.genFilePath(), commandBytes);

            // 添加索引
//            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
//            index.put(key, cmdPos);
            //确保不出意外查不到已经删除的数据
            index.remove(key);
            operateNums++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }
    //回放功能
    @Override
    public void reDoLog() {
        zipFile();
        index.clear();
        reloadIndex();
    }
    //多线程执行reDOLog
    public void multStarReDolog(){
        int numberOfThreads = 3; // 想要执行的 reDoLog 方法的数量

        // 创建一个固定大小的线程池
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        // 提交多个 reDoLog 方法给线程池执行
        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> {
                this.reDoLog();
            });
        }

        // 关闭线程池
        executor.shutdown();
    }
    @Override
    public void close() {
        try {
            if (writerReader != null) {
                writerReader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("关闭 RandomAccessFile 时出现异常", e);
        }
    }

}
