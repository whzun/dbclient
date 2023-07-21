运行脚本：
./hive.sh [options]
options 说明如下：
 -d,--driver <arg>     Driver class name, default
                       "org.apache.hive.jdbc.HiveDriver"
 -f,--file <arg>       SQL file name
 -h,--help             Print this usage
 -i,--ip <arg>         Hive server ip address
 -P,--port <arg>       Server port number, default "10000"
 -p,--password <arg>   Password
 -s,--sql <arg>        SQL script, if "f" is specified this option will be
                       ignored
 -u,--user <arg>       User name


交互模式说明
进入系统后出现输入提示：
SQL>
输入一行或多行sql，最终以分号结束sql，如
    select col1,col2
    from table_name
    order by col1;
查询语句结果分页输出，回车进入下一页，输入q退出

如果需要退出客户端，直接输入“quit”命令。
