package com.alibaba.middleware.race.sync;

import java.util.HashMap;

interface Cons {

    // '0'
    byte CHAR_ZERO = 48;

    //实际长度|VAL_LEN

    //各属性长度

    byte VAl1_LEN = 3;
    byte VAL1_OFFSET = 0;

    byte VAL2_LEN = 6;
    byte VAL2_OFFSET = VAl1_LEN ;

    byte VAL3_LEN = 3;
    byte VAL3_OFFSET = VAl1_LEN + VAL2_LEN ;

    byte VAL4_LEN = 4;
    byte VAL4_OFFSET = VAl1_LEN + VAL2_LEN + VAL3_LEN ;

    byte VAL5_LEN = 6;
    byte VAL5_OFFSET = VAl1_LEN + VAL2_LEN + VAL3_LEN + VAL4_LEN ;

    byte LEN = VAl1_LEN + VAL2_LEN + VAL3_LEN + VAL4_LEN + VAL5_LEN ;

    byte[] VAL_LEN_ARRAY = new byte[]{VAl1_LEN,VAL2_LEN,VAL3_LEN,VAL4_LEN,VAL5_LEN};
    byte[] VAL_OFFSET_ARRAY = new byte[]{VAL1_OFFSET,VAL2_OFFSET,VAL3_OFFSET,VAL4_OFFSET,VAL5_OFFSET};

    byte KEY1_LEN = 14;

    byte KEY2_LEN = 13;

    byte KEY3_LEN = 7;

    byte KEY4_LEN = 9;

    byte KEY5_LEN = 10;


    byte[] KEY_LEN_ARRAY = new byte[]{KEY1_LEN,KEY2_LEN,KEY3_LEN,KEY4_LEN,KEY5_LEN};


    int PK_NAME_LEN = 6;




    //    属性约定表,用索引指代属性(赛题)
    HashMap<Integer, Byte> KEYMAP = new HashMap<Integer, Byte>(4) {
        {
            //[4]+[5]

//            first_name:2:0
            put('t' + '_', (byte) 0);
//            last_name:2:0
            put('_' + 'n', (byte) 1);
//            sex:2:0
            put('2' + ':', (byte) 2);
//            score:1:0
            put('e' + ':', (byte) 3);
//            score2:1:0
            put('e' + '2', (byte) 4);
        }
    };


//    HashMap<Byte, byte[]> DECODEMAP = new HashMap<Byte, byte[]>(4) {
//        {
//            put((byte) 0, "first_name".getBytes(CHARSET));
//            put((byte) 1, "last_name".getBytes(CHARSET));
//            put((byte) 2, "sex".getBytes(CHARSET));
//            put((byte) 3, "score".getBytes(CHARSET));
//            put((byte) 4, "score2".getBytes(CHARSET));
//        }
//    };











}
