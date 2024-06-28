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
}
