package test;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

public class FileParser5Tester {

    public static void main(String[] args) {
//        FileParser5 fileParser = new FileParser5();
//        for (int i = 0; i < 10; i++) {
//            fileParser.readPage((byte) 1);
//            fileParser.mergeResult();
//            System.out.println("has parsed page: " + i);
//        }
//        fileParser.showResult();

        HashIntIntMap map = HashIntIntMaps.newMutableMap();
        map.put(-100000,1);
        System.out.println(map.get(-100000));

    }
}
