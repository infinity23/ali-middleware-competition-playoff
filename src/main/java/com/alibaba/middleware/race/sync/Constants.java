package com.alibaba.middleware.race.sync;

import java.nio.charset.Charset;

/**
 * 外部赛示例代码需要的常量 Created by wanshao on 2017/5/25.
 */
interface Constants {

    Charset CHARSET = Charset.forName("UTF-8");
    byte EN = 10;

    // ------------ 本地测试可以使用自己的路径--------------//

//     工作主目录
     String TESTER_HOME = "D:\\IncrementalSync\\data";
//     赛题数据
    String DATA_HOME = "D:\\IncrementalSync\\data\\";
    // 结果文件目录
    String RESULT_HOME = "D:\\IncrementalSync\\data\\result\\";
    // 中间结果目录
    String MIDDLE_HOME = "D:\\IncrementalSync\\data\\middle\\";



    // server端口
    Integer SERVER_PORT = 5527;
    // teamCode
    String TEAMCODE = "76870yg5no";
    // 日志级别
    String LOG_LEVEL = "INFO";

    //     结果文件的命名
     String RESULT_FILE_NAME = "Result.rs";
    // ------------ 正式比赛指定的路径--------------//

//    //// 工作主目录
//    String TESTER_HOME = "/home/admin/";
//    // 赛题数据
//     String DATA_HOME = "/home/admin/canal_data/";
//    // 结果文件目录(client端会用到)
//    String RESULT_HOME = "/home/admin/sync_results/76870yg5no/";
//    // 中间结果目录（client和server都会用到）
//     String MIDDLE_HOME = "/home/admin/middle/76870yg5no/";
////     结果文件的命名
//     String RESULT_FILE_NAME = "Result.rs";

}
