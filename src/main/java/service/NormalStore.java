/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
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
    private static final int THRESHOLD_FOR_PERSISTENCE = 30;

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
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
                if (cmdLen <= 0) {
                    break; // Exit the loop if cmdLen is non-positive
                }
                byte[] bytes = new byte[cmdLen];
                int bytesRead = file.read(bytes);
                if (bytesRead == -1) {
                    break; // Exit the loop if end of file is reached unexpectedly
                }
                if (bytesRead != cmdLen) {
                    break; // Exit the loop to avoid incorrect processing
                }
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                System.out.println(value);
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                }
                start += cmdLen;
//                start += 4 + cmdLen; // 更新起始位置，加上命令长度和命令长度字段的长度
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
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);//返回偏移量
            // 保存到memTable
            memTable.put(key, command); // 将指令存入内存表
            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            // TODO://判断是否需要将内存表中的值写回table
            System.out.println(memTable.size());
            if (memTable.size() >= THRESHOLD_FOR_PERSISTENCE) {
                persistMemTable(); // 持久化内存表到磁盘
                System.out.println("已经持久化内存表到磁盘！");
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }
    // 将内存表数据持久化到磁盘
    private void persistMemTable() {
        try {
            // 逐个将内存表中的指令写入磁盘
            for (Map.Entry<String, Command> entry : memTable.entrySet()) {
                String key = entry.getKey();
                Command command = entry.getValue();
                byte[] commandBytes = JSONObject.toJSONBytes(command);

                RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
                int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);

                CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
                index.put(key, cmdPos);
            }
            // 清空内存表
            memTable.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

            JSONObject value = JSONObject.parseObject(new String(commandBytes));
            Command cmd = CommandUtil.jsonToCommand(value);
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
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘

            // 写table（wal）文件，返回偏移量
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 保存到memTable
            memTable.put(key, command); // 将指令存入内存表

            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);

            // TODO://判断是否需要将内存表中的值写回table
            if (memTable.size() >= THRESHOLD_FOR_PERSISTENCE) {
                persistMemTable(); // 持久化内存表到磁盘
                System.out.println("已经持久化内存表到磁盘！");
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (writerReader != null) {
                writerReader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("关闭 RandomAccessFile 时出现异常", e);
        }
    }

}
