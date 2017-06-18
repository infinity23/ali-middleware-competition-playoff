package com.alibaba.middleware.race.sync;/*
package com.alibaba.middleware.race.sync;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Resolver {
    private String dataPath;
    private String file = "/canal.txt";
    private String resultPath;
    private String schema;
    private String table;
    private int lo;
    private int hi;
    private ExecutorService executorService = Executors.newFixedThreadPool(Constants.THREAD_NUM);

    private int lastPoint;

    RandomAccessFile randomAccessFile;


    public Resolver(String dataPath, String resultPath, String schema, String table, int lo, int hi) {
        this.dataPath = dataPath;
        this.resultPath = resultPath;
        this.schema = schema;
        this.table = table;
        this.lo = lo;
        this.hi = hi;

        try {
            randomAccessFile = new RandomAccessFile(dataPath + file, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void readFile(Result result) {
        try {
            int start = lastPoint;
            int end;
            for (int i = 0; i < Constants.THREAD_NUM; i++) {
                int skipSize = randomAccessFile.skipBytes(Constants.BLOCK_SIZE);
                end = start + skipSize;
                if (skipSize == Constants.BLOCK_SIZE) {
                    while (randomAccessFile.read() != Constants.EN) {
                        end++;
                    }
                    end++;
                }
                byte[] data = new byte[end - start];
                randomAccessFile.seek(start);
                randomAccessFile.read(data);
                Future<Result> future = executorService.submit(new Parser(data, result));
                futureList.add(future);
                start = end;
                if (start >= randomAccessFile.length()) {
                    break;
                }
            }

            lastPoint = (int) randomAccessFile.getFilePointer();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void readFileByReader(Result result) {
        try {
            FileInputStream fileInputStream = new FileInputStream(dataPath + file);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, Constants.CHARSET);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String s;
            String[] data = new String[Constants.CACHE_SIZE];
            int index = 0;

            while ((s = bufferedReader.readLine()) != null) {
                data[index++] = s;
                if (index == data.length) {
                    Future<Result> future = executorService.submit(new Parser2(data, result));
                    futureList.add(future);
                    data = new String[Constants.CACHE_SIZE];
                    index = 0;
                }
            }
            String[] last = new String[index];
            System.arraycopy(data,0,last,0,index);
            Future<Result> future = executorService.submit(new Parser2(last, result));
            futureList.add(future);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private class Parser implements Callable<Result> {
        private byte[] data;

        public Parser(byte[] data) {
            this.data = data;
        }

        @Override
        public Result call(){
            HashMap<Integer, HashMap<String, String>> updateMap = new HashMap<>();
            HashSet<Integer> deleteSet = new HashSet<>();
            HashMap<Integer, FilePointer> insertMap = new HashMap<>();
            LinkedHashMap<Integer, Integer> PKChangeMap = new LinkedHashMap<>();
//            if(result != null) {
//                updateMap = result.getUpdateMap();
//                deleteSet = result.getDeleteSet();
//                recordMap = result.getInsertMap();
//                PKChangeMap = new LinkedHashMap<>();
//            }else {
//                updateMap = new HashMap<>();
//                deleteSet = new HashSet<>();
//                recordMap = new HashMap<>();
//                PKChangeMap = new LinkedHashMap<>();
//            }
            int start;
            int end;
            char operation;
            int pk;

            //解析数据
            for (int i = data.length - 1; i >= 0; i--) {
                end = i;
                start = skipToStart(i);
                i = start;
                String s = new String(data, start, end - start, Constants.CHARSET);

                //正则匹配，可优化
                String[] ss = s.substring(1, s.length() - 1).split("\\|");
                //检测库表
                if (!ss[2].equals(schema) || !ss[3].equals(table)) {
                    continue;
                }

                operation = ss[4].charAt(0);

                if (operation != 'I') {
                    pk = Integer.parseInt(ss[6]);
                } else {
                    pk = Integer.parseInt(ss[7]);
                }

//                无法处理主键更改的情况
//                if (!(pk > lo && pk < hi)) {
//                    continue;
//                }


                //处理操作
                if (operation == 'U') {
                    if (deleteSet.contains(pk)) {
                        continue;
                    }

                    //处理改变主键的情况
                    if (!ss[6].equals(ss[7])) {
                        int newPk = Integer.parseInt(ss[7]);

//                        if (!(newPk > lo && newPk < hi)) {
//                            continue;
//                        }

                        if (deleteSet.contains(newPk)) {
                            continue;
                        }
                        PKChangeMap.put(pk, newPk);

                        //是否同时变更了数据
                        if (ss.length == 8) {
                            continue;
                        }
                    }

                    String name = parseName(ss[8]);

                    if (updateMap.containsKey(pk)) {
                        if (updateMap.get(pk).containsKey(name)) {
                            continue;
                        }
                        updateMap.get(pk).put(name, ss[10]);
                    } else {
                        HashMap<String, String> map = new HashMap<>();
                        map.put(name, ss[10]);
                        updateMap.put(pk, map);
                    }

                } else if (operation == 'I') {
                    if (deleteSet.contains(pk)) {
                        continue;
                    }

                    LinkedHashMap<String, String> map = new LinkedHashMap<>();

                    for (int j = 8; j < ss.length - 2; j++) {
                        String name = parseName(ss[j]);
                        String value = ss[j + 2];
                        map.put(name, value);
                        j += 2;
                    }
                    insertMap.put(pk, map);

                } else if (operation == 'D') {
                    deleteSet.add(pk);
                    insertMap.remove(pk);
                    updateMap.remove(pk);
                }
            }
            return new Result(insertMap, updateMap, deleteSet, PKChangeMap);
        }

        //倒着解析
        private int skipToStart(int index) {
            while (index > 0 && data[index - 1] != Constants.EN) {
                index--;
            }
            return index;
        }

        private String parseName(String s) {
            int i = s.indexOf(":");
            return s.substring(0, i);
        }

    }

    private class Parser2 implements Callable<Result> {
        private String[] data;
        private Result result;

        public Parser2(String[] data, Result result) {
            this.data = data;
            this.result = result;
        }

        @Override
        public Result call() throws Exception {
            HashMap<Integer, HashMap<String, String>> updateMap;
            HashSet<Integer> deleteSet;
            HashMap<Integer, FilePointer> recordMap;
            LinkedHashMap<Integer, Integer> PKChangeMap;

            if(result != null) {
               updateMap = result.getUpdateMap();
               deleteSet = result.getDeleteSet();
              recordMap = result.getInsertMap();
               PKChangeMap = new LinkedHashMap<>();
            }else {
                updateMap = new HashMap<>();
                deleteSet = new HashSet<>();
                recordMap = new HashMap<>();
               PKChangeMap = new LinkedHashMap<>();
            }

            char operation;
            int pk;

            //解析数据
            for (int i = data.length - 1; i >= 0; i--) {
                String s = data[i];

                //正则匹配，可优化
                String[] ss = s.substring(1, s.length() - 1).split("\\|");
                //检测库表
                if (!ss[2].equals(schema) || !ss[3].equals(table)) {
                    continue;
                }

                operation = ss[4].charAt(0);

                if (operation != 'I') {
                    pk = Integer.parseInt(ss[6]);
                } else {
                    pk = Integer.parseInt(ss[7]);
                }

//                无法处理主键更改的情况
//                if (!(pk > lo && pk < hi)) {
//                    continue;
//                }


                //处理操作
                if (operation == 'U') {
                    if (deleteSet.contains(pk)) {
                        continue;
                    }

                    //处理改变主键的情况
                    if (!ss[6].equals(ss[7])) {
                        int newPk = Integer.parseInt(ss[7]);

//                        if (!(newPk > lo && newPk < hi)) {
//                            continue;
//                        }

                        if (deleteSet.contains(newPk)) {
                            continue;
                        }
                        PKChangeMap.put(pk, newPk);

                        //是否同时变更了数据
                        if (ss.length == 8) {
                            continue;
                        }
                    }

                    String name = parseName(ss[8]);

                    if (updateMap.containsKey(pk)) {
                        if (updateMap.get(pk).containsKey(name)) {
                            continue;
                        }
                        updateMap.get(pk).put(name, ss[10]);
                    } else {
                        HashMap<String, String> map = new HashMap<>();
                        map.put(name, ss[10]);
                        updateMap.put(pk, map);
                    }

                } else if (operation == 'I') {
                    if (deleteSet.contains(pk)) {
                        continue;
                    }

                    FilePointer map = new LinkedHashMap<>();

                    for (int j = 8; j < ss.length - 2; j++) {
                        String name = parseName(ss[j]);
                        String value = ss[j + 2];
                        map.put(name, value);
                        j += 2;
                    }
                    recordMap.put(pk, map);

                } else if (operation == 'D') {
                    deleteSet.add(pk);
                    recordMap.remove(pk);
                    updateMap.remove(pk);
                }
            }
            return new Result(recordMap, updateMap, deleteSet, PKChangeMap);
        }

        private String parseName(String s) {
            int i = s.indexOf(":");
            return s.substring(0, i);
        }

    }

    //合并一页结果
    //正序，后面会覆盖前面
    public Result mergeResult() {
//        HashMap<Integer, HashMap<String,String>> updateMap = new HashMap<>(results.get(results.size()-1).getUpdateMap());
        HashMap<Integer, HashMap<String, String>> updateMap = new HashMap<>();
        HashSet<Integer> deleteSet = new HashSet<>();
        HashMap<Integer, FilePointer> recordMap = new HashMap<>();

        //从前往后取出结果
        while (!futureList.isEmpty()) {
            Future<Result> future = futureList.poll();
            Result result = null;
            try {
                result = future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            HashMap<Integer, HashMap<String, String>> uMap = result.getUpdateMap();
            HashSet<Integer> dSet = result.getDeleteSet();
            HashMap<Integer, FilePointer> rMap = result.getInsertMap();
            LinkedHashMap<Integer, Integer> PKChangeMap = result.getPKChangeMap();

            //合并updateMap
            for (Integer pk : uMap.keySet()) {
                if (updateMap.containsKey(pk)) {
                    updateMap.get(pk).putAll(uMap.get(pk));
                } else {
                    updateMap.put(pk, uMap.get(pk));
                }

            }

            //合并recordMap
            for (Integer pk : rMap.keySet()) {
                recordMap.put(pk, rMap.get(pk));
            }

            //合并deleteSet
            deleteSet.addAll(dSet);

            //处理主键变更
            ListIterator<Integer> it = new ArrayList<>(PKChangeMap.keySet()).listIterator(PKChangeMap.size());
            while (it.hasPrevious()) {
                int pk = it.previous();
                int newPK = PKChangeMap.get(pk);
                recordMap.put(newPK, recordMap.get(pk));
                if (updateMap.containsKey(pk)) {
                    updateMap.put(newPK, updateMap.get(pk));
                    if (uMap.containsKey(newPK)) {
                        updateMap.get(newPK).putAll(uMap.get(newPK));
                    }
                }
                deleteSet.remove(newPK);
                deleteSet.add(pk);
            }

        }

        //更新umap和rmap
        for (Integer i : deleteSet) {
            updateMap.remove(i);
            recordMap.remove(i);
        }

        return new Result(recordMap, updateMap, deleteSet, null);
    }


    //    计算最后结果
    public void showResult(Result result) {

        HashMap<Integer, FilePointer> recordMap = result.getInsertMap();
        HashMap<Integer, HashMap<String, String>> updateMap = result.getUpdateMap();

        //umap更新到rmap
        for (Integer i : updateMap.keySet()) {
            if (recordMap.containsKey(i)) {
                FilePointer record = recordMap.get(i);
                for (Map.Entry<String, String> entry : updateMap.get(i).entrySet()) {
                    record.put(entry.getKey(), entry.getValue());
                }
            }
        }

        try {
            FileWriter fileWriter = new FileWriter(resultPath + "/MyResult.rs");
            StringBuilder stringBuilder = new StringBuilder();

            ArrayList<Integer> pks = new ArrayList<>(recordMap.keySet());
            Collections.sort(pks);
            Iterator<Integer> it = pks.iterator();

            while (it.hasNext()) {
                int pk = it.next();
//                检测范围
                if (pk >= hi || pk <= lo) {
                    continue;
                }
                FilePointer record = recordMap.get(pk);
                stringBuilder.append(pk);
                for (Map.Entry<String, String> keyValue : record.entrySet()) {
                     stringBuilder.append('\t');
                    stringBuilder.append(keyValue.getValue());
                }
                stringBuilder.append('\n');
                fileWriter.write(stringBuilder.toString());
                stringBuilder.delete(0, stringBuilder.length());
            }
            fileWriter.close();
            executorService.shutdown();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
*/
