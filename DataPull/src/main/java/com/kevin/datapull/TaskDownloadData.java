package com.kevin.datapull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TaskDownloadData
{

    public static void main(String[] args)
    {

        int threadCount = Integer.parseInt(args[0]); // 线程个数
        String filePath = args[1];

        String mysqlTable = args[2];
        String rule = args[3]; // 表名后缀规则，name_2015_1 n_yyyym, name_201501 n_yyyymm, name n

        String fields = args[4];
        String where = args[5];
        String shellPath = args[6];
        String tmpDataPath = args[7]; // 下载的数据的存放目录，这里建议写明游戏名称

        // 获取最终需要查询数据的表
        System.out.println("filePath: " + filePath);
        System.out.println("mysqlTable: " + mysqlTable);
        System.out.println("rule: " + rule);
        System.out.println("fields: " + fields);
        System.out.println("where: " + where);
        System.out.println("shellPath: " + shellPath);
        System.out.println("tmpDataPath: " + tmpDataPath);

        Set<String> fixTables = fixTables(mysqlTable, rule, where);
        System.out.println("要拉取数据的表为:  " + fixTables);

        long downloadStart = System.currentTimeMillis();
        System.out.println("开始拉数据...............");

        CountDownLatch latch = new CountDownLatch(threadCount);
        //  ConcurrentLinkedQueue<String> taskCounts = TaskGetUrl.getTaskCount(filePath);

        ConcurrentLinkedQueue<String> taskCounts = null;
        System.out.println("要拉取的数据库有: " + taskCounts.size());

        ExecutorService esDown = Executors.newFixedThreadPool(threadCount);
        TaskDataThread taskDataThread = new TaskDataThread();
        taskDataThread.setTaskCountQueue(taskCounts, latch, mysqlTable, fields, where, shellPath, tmpDataPath, fixTables);

        for (int i = 0; i < threadCount; i++)
        {
            esDown.submit(taskDataThread);
        }
        try
        {
            latch.await(); // 使得主线程(main)阻塞直到latch.countDown()为零才继续执行
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        System.out.println("拉取数据时间共消耗 ： " + (System.currentTimeMillis() - downloadStart) / 1000 + " 秒");
        esDown.shutdown();

        System.out.println("拉取数据成功的地址有 ： " + taskDataThread.getSuccessUrl().size());
        System.out.println("拉取连接失败的地址有 ： " + taskDataThread.getFailUrl().size());
        if (taskDataThread.getFailUrl().size() != 0)
        {
            // System.out.println(DateUtil.getCurrentTime() + " 连接失败的地址有 ： ");
            for (String string : taskDataThread.getFailUrl())
            {
                System.out.println(string);
                System.out.println();
            }
            System.exit(-1);
        }
    }

    public static Set<String> fixTables(String mysqlTable, String rule, String where)
    {

        Set<String> tables = new HashSet<String>();

        if (where.equals("1=1"))
        {
            tables.add(mysqlTable);
            return tables;
        }

        // 根据正则获取到日期，然后计算日期之间的月份，并获取加一个月和减一个月的月份
        Pattern pattern = Pattern.compile(" .*(.*)>='(.*)' and (.*)<='(.*)'.* ");
        Matcher matcher = pattern.matcher(where);
        String startDate = null;
        String endDate = null;
        while (matcher.find())
        {
            startDate = matcher.group(2);
            endDate = matcher.group(4);
        }

        // List<String> monthBetween = DateUtil.getMonthBetween(DateUtil.monthLess1(startDate), DateUtil.monthAdd1(endDate));
        List<String> monthBetween = new ArrayList<>();

        if (rule.equals("n"))
        {
            tables.add(mysqlTable);
        }
        else if (rule.equals("n_yyyymm"))
        {
            for (String string : monthBetween)
            {
                String table = mysqlTable + "_" + string.replace("-", "");
                tables.add(table);
            }
        }
        else if (rule.equals("n_yyyym"))
        {
            for (String string : monthBetween)
            {
                String[] split = string.split("-");
                String month = split[0] + "_" + Integer.parseInt(split[1]);
                String table = mysqlTable + "_" + month;
                tables.add(table);
            }
        }

        return tables;
    }
}
