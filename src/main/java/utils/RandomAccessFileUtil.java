/*
 *@Type RandomAccessFileUtil.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:58
 * @version
 */
package utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class RandomAccessFileUtil {

    private static final String RW_MODE = "rw";
//将字节数组 value 写入指定文件 filePath 的末尾。
    public static int write(String filePath, byte[] value) {
        RandomAccessFile file = null;
        long len = -1L;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            len = file.length();
            file.seek(len);
            file.write(value);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (int)len;
    }
    //将整数 value 写入指定文件 filePath 的末尾。
    public static void writeInt(String filePath, int value) {
        RandomAccessFile file = null;
        long len = -1L;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            len = file.length();
            file.seek(len);
            file.writeInt(value);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//从指定文件 filePath 的 index 位置开始读取长度为 len 的字节数据。
    public static byte[] readByIndex(String filePath, int index, int len) {
        RandomAccessFile file = null;
        byte[] res = new byte[len];
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            file.seek((long)index);
            file.read(res, 0, len);
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}
