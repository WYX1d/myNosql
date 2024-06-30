/*
 *@Type CommandPos.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:35
 * @version
 */
package model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CommandPos {
    private int pos;
    private int len;

    public CommandPos(int pos, int len) {
        this.pos = pos;//数据起始位置
        this.len = len;//数据长度
    }

    @Override
    public String toString() {
        return "CommandPos{" +
                "pos=" + pos +
                ", len=" + len +
                '}';
    }
}
