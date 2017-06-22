package com.alibaba.middleware.race.sync;

import java.nio.charset.Charset;

interface Constants {

    Charset CHARSET = Charset.forName("UTF-8");
    //'\n'
    byte EN = 10;
    //'|'
    byte SP = 124;
    //':'
    byte CO = 58;

    int DIRECT_CACHE = 200 * 1024 * 1024;

    // ------------ 本地测试可以使用自己的路径--------------//
////         工作主目录
//     String TESTER_HOME = "E:\\Major\\IncrementalSync\\example";
////     赛题数据
//    String DATA_HOME = "E:\\Major\\IncrementalSync\\example\\";
//    // 结果文件目录
//    String RESULT_HOME = "E:\\Major\\IncrementalSync\\example\\result\\";
//    // 中间结果目录
//    String MIDDLE_HOME = "E:\\Major\\IncrementalSync\\example\\middle\\";
//    //        middleware5 student 100 200
//    String SCHEMA = "middleware5";
//    String TABLE = "student";
//    int LO = 100;
//    int HI = 200;
//    int BLOCK_SIZE = 20 * 1024 * 1024;


//    //     工作主目录
//    String TESTER_HOME = "E:\\Major\\IncrementalSync\\";
//    //     赛题数据
//    String DATA_HOME = "E:\\Major\\IncrementalSync\\last\\";
//    // 结果文件目录
//    String RESULT_HOME = "E:\\Major\\IncrementalSync\\result\\";
//    // 中间结果目录
//    String MIDDLE_HOME = "E:\\Major\\IncrementalSync\\middle\\";
//
//    //    middleware5 student 100000 2000000
//    String SCHEMA = "middleware5";
//    String TABLE = "student";
//    int LO = 100000;
//    int HI = 2000000;
//    int BLOCK_SIZE = 100 * 1024 * 1024;
//    int CACHE_SIZE = 200 * 1024 * 1024;
//    int THREAD_NUM = 4;
//
//
//    int KEY_NUM = 4;
//    //后缀 keys43 + nulls16 + |12 + 5 + values最小（3 + 3 + 3 + score(1) ）10
//    int SUFFIX = 86;


    // ------------ 正式比赛指定的路径--------------//

    //    middleware8 student 1000000 8000000
    String SCHEMA = "middleware8";
    String TABLE = "student";
    int LO = 1000000;
    int HI = 8000000;

    int KEY_NUM = 5;
    int BLOCK_SIZE = 100 * 1024 * 1024;
    int CACHE_SIZE = 300 * 1024 * 1024;
    int THREAD_NUM = 32;

    //后缀 keys53 + nulls20 + |15 + 5 + values最小（3 + 3 + 3 + score(1) + score2(1)）11
    int SUFFIX = 104;

    // server端口
    Integer SERVER_PORT = 5527;
    // teamCode
    String TEAMCODE = "76870yg5no";
    // 日志级别
    String LOG_LEVEL = "INFO";

    //     结果文件的命名
    String RESULT_FILE_NAME = "Result.rs";


    // 工作主目录
    String TESTER_HOME = "/home/admin";
    // 赛题数据
    String DATA_HOME = "/home/admin/canal_data/";
    // 结果文件目录(client端会用到)
    String RESULT_HOME = "/home/admin/sync_results/76870yg5no/";
    // 中间结果目录（client和server都会用到）
    String MIDDLE_HOME = "/home/admin/middle/76870yg5no/";


}
