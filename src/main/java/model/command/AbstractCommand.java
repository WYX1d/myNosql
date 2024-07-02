/*
 *@Type AbstractCommand.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 01:51
 * @version
 */
package model.command;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class AbstractCommand implements Command {
    /*
    * 命令类型
    * */
    private CommandTypeEnum type;

    public AbstractCommand(CommandTypeEnum type) {
        this.type = type;
    }
//将当前对象转换为 JSON 格式的字符串
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
    public byte[] toByte() {
        byte[] bytes = JSON.toJSONBytes(this);
        byte[] newBytes = new byte[bytes.length + 2]; // 新数组长度 = 原数组长度 + 2（回车符和换行符）
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length); // 复制原数组到新数组
        newBytes[bytes.length] = (byte) '\r'; // 添加回车符
        newBytes[bytes.length + 1] = (byte) '\n'; // 添加换行符
        return newBytes;
    }
}
