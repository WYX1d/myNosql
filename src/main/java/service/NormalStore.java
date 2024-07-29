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
//自动rotate，缓存，索引优化，压缩、多线程（后台实现）。servelet restful api,junit压测。
//内存表在停止程序前要保存到磁盘，索引优化(压缩、内存表、删除)

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import model.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author wyx
 */
public class NormalStore implements Store {

    //    public static final String TABLE = ".txt";
    public static String TABLE = ".txt";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";


    /**
     * 内存表，类似缓存
     */
    @Getter
    private TreeMap<String, Command> memTable;
    private TreeMap<String, Command> getmemTable;

    /**
     * hash索引，存的是数据长度和偏移量
     */
    @Getter
    private HashMap<String, CommandPos> index;
    //定义一个数组存储各个文件的索引
    @Getter
    private ArrayList<HashMap<String, CommandPos>> indexs;

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
    private static final int fileNums = 4;
    /**
     * 单个文件存储阀值
     */
    private static final int maxFileSize = 10;
    /**
     * 内存表阈值
     */
    private static final int memTableSize = 3;

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap();

        this.getmemTable = new TreeMap();
        this.index = new HashMap<>();
        this.indexs = new ArrayList<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadAllIndex();
    }

    //生成当前操作的文件名
    public String genFilePath() {
        //默认文件后缀为1
        int number = 1;
        StringBuilder fileNameBuilder = new StringBuilder(this.dataDir);
        fileNameBuilder.append(File.separator).append(NAME).append(number);
        String fileName = fileNameBuilder.toString();

        Path path = Paths.get(fileName);
        int lineCount = 0;
        try {
            lineCount = Files.readAllLines(path).size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 当前文件已满时更改文件后缀
        while (lineCount >= maxFileSize) {
            number++;
            fileNameBuilder.setLength(this.dataDir.length() + NAME.length() + 1);  // 重置文件路径
            fileNameBuilder.append(number);

            try {
                Path path1 = Paths.get(fileNameBuilder.toString());
                lineCount = Files.readAllLines(path1).size();
            } catch (IOException a) {
                //当前文件不存在时直接返回
                return fileNameBuilder.toString();
            }
        }
        String thisfileName = fileNameBuilder.toString();
        return thisfileName;
    }

//加载全部数据到索引

    public void reloadAllIndex() {
        StringBuilder fileNameBuilder = new StringBuilder(this.dataDir + File.separator + NAME);
        int fileIndex = 0;
        while (true) {
            fileIndex++;
            fileNameBuilder.setLength(this.dataDir.length() + NAME.length() + 1);  // 重置文件路径
            fileNameBuilder.append(fileIndex);
            String thisfileName = fileNameBuilder.toString();
            if (!new File(thisfileName).exists()) {
                System.out.println(indexs.toString());
                return;
            }
            //加载其他文件的索引
            this.reloadIndex(thisfileName);
        }

    }

    public void reloadIndex(String fileName) {
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, RW_MODE);
            long len = file.length();
            if (len < 3) {
                // 文件为空，跳过操作
                return;
            }
            //创建当前文件的索引
            HashMap<String, CommandPos> tolIndex = new HashMap<>();
            index.clear();
            long start = 0;
            file.seek(start);
            while (start < len - 2) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                    tolIndex.put(command.getKey(), cmdPos);
                }
                start += cmdLen;
            }
            indexs.add(tolIndex);
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
    }

    //增改操作
    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            if (memTable.size() >= memTableSize) {
                //将内存表写入磁盘
                flush();
                memTable.clear();
            }
            memTable.put(key, command);
            getmemTable.put(key, command);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    //查询数据
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            //先从当前内存表中查找
            Command command = getmemTable.get(key);
            if (command != null) {
                if (command instanceof SetCommand) {
                    return ((SetCommand) command).getValue();
                }
                if (command instanceof RmCommand) {
                    return null;
                }
            }

            //内存表找不到再到通过索引在磁盘查找
            // 遍历index
            if (indexs.size() == 0) {
                return null;
            }
            for (int i = indexs.size() - 1; i >= 0; i--) {
                HashMap<String, CommandPos> map = indexs.get(i);
                System.out.println(map.toString());

                if (!map.containsKey(key)) {
                    continue;
                }
                CommandPos cmdPos = map.get(key);
                if (cmdPos == null) {
                    return null;
                }

                StringBuilder fileNameBuilder = new StringBuilder(this.dataDir);
                fileNameBuilder.append(File.separator).append(NAME).append(i + 1);
                String fileName = fileNameBuilder.toString();

                byte[] commandBytes = RandomAccessFileUtil.readByIndex(fileName, cmdPos.getPos(), cmdPos.getLen());
                System.out.println(commandBytes.toString());
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
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            if (memTable.size() >= memTableSize) {
                //将内存表写入磁盘
                flush();
                memTable.clear();
            }
            memTable.put(key, command);
            getmemTable.put(key, command);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    //压缩磁盘中的数据
    public void zipFile() {
        int numberOfThreads = indexs.size() + 5;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        StringBuilder fileNameBuilder = new StringBuilder(this.dataDir + File.separator + NAME);
        int fileIndex = 0;
        while (true) {
            fileIndex++;
            fileNameBuilder.setLength(this.dataDir.length() + NAME.length() + 1);  // 重置文件路径
            fileNameBuilder.append(fileIndex);
            String thisfileName = fileNameBuilder.toString();
            if (!new File(thisfileName).exists()) {
                break;
            }
            executor.execute(new Rotate(thisfileName));
        }
        reloadAllIndex();
    }

    //回放功能
    @Override
    public void reDoLog() {
//        zipFile();
        //加载索引
        index.clear();
        reloadIndex(this.genFilePath());
    }

    //多线程执行reDOLog
    public void multStarReDolog() {
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

    public void flush() {
        for (Map.Entry<String, Command> entry : memTable.entrySet()) {
            AbstractCommand cmd = null;
            if (entry.getValue() instanceof SetCommand) {
                cmd = (SetCommand) entry.getValue();
            } else if (entry.getValue() instanceof RmCommand) {
                cmd = (RmCommand) entry.getValue();
            }
            byte[] cmdBytes = cmd.toByte();
            String filePath = this.genFilePath();
            RandomAccessFileUtil.writeInt(filePath, cmdBytes.length);
            int pos = RandomAccessFileUtil.write(filePath, cmdBytes);//返回偏移量
        }
        //先判断文件容量是否需要压缩
        int lineCount = FileCount();

        String fileName = this.genFilePath();
        String numberPart = fileName.replaceAll("^.*?(\\d+)$", "$1");
        int number = Integer.parseInt(numberPart);
        //文件数据一定时进行压缩
        if (lineCount - 1 >= maxFileSize&&number>=fileNums) {
            System.out.println("文件容量达到最大值，开始压缩文件");
            zipFile();
        }
    }

    //获取文件行数
    public int FileCount() {
        Path path = Paths.get(this.genFilePath());
        int lineCount = 0;
        try {
            lineCount = Files.readAllLines(path).size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lineCount;
    }

    //程序异常关闭时将内存表写入磁盘
    @Override
    public void close() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("程序正在关闭...");
            // 在这里执行一些清理操作
            flush();
            if (writerReader != null) {
                try {
                    writerReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
    }
}
