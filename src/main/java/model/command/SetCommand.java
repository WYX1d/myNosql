/*
 *@Type SetCommand.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 01:59
 * @version
 */
package model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter


public class SetCommand extends AbstractCommand {
    // 声明一个私有字符串变量来存储键
    private String key;

    // 声明一个私有字符串变量来存储值
    private String value;

    // 构造函数，用于初始化键和值
    public SetCommand(String key, String value) {
        // 调用超类构造函数，使用SET命令类型
        super(CommandTypeEnum.SET);
        // 设置键和值变量
        this.key = key;
        this.value = value;
    }
}


