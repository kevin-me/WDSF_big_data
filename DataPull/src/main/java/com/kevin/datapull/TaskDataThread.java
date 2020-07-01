package com.kevin.datapull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * 线程类，用来处理缓存到队列里面的数据库地址的类
 *
 * @author hutao
 */
public class TaskDataThread implements Runnable
{

    /**
     * 接收的 mysql 地址队列
     */
    private ConcurrentLinkedQueue<String> taskCountQueue = null;
    /**
     * CountDownLatch，一个同步辅助类，在完成一组正在其他线程中执行的操作之前，它允许一个或多个线程一直等待。
     */
    private CountDownLatch latch = null;

    private static List<String> successUrl = new ArrayList<>();
    private static List<String> failUrl = new ArrayList<>();

    private String mysqlTable = null;
    private String fields = null;
    private String where = null;
    private String shellPath = null;
    private String tmpDataPath = null;
    private Set<String> fixTables = null;

    public ConcurrentLinkedQueue<String> getTaskCountQueue()
    {
        return taskCountQueue;
    }

    public void setTaskCountQueue(ConcurrentLinkedQueue<String> taskCountQueue, CountDownLatch latch,
                                  String mysqlTable, String fields, String where, String shellPath, String tmpDataPath, Set<String> fixTables)
    {
        this.taskCountQueue = taskCountQueue;
        this.latch = latch;
        this.mysqlTable = mysqlTable;
        this.fields = fields;
        this.where = where;
        this.shellPath = shellPath;
        this.tmpDataPath = tmpDataPath;
        this.fixTables = fixTables;
    }

    public void run()
    {
        while (true)
        {
//			synchronized (TaskDataThread.class) {
            String poll = taskCountQueue.poll();
            if (poll == null)
            {
                break;
            }
            String[] split = poll.split("\t");
            String plat = split[0].trim();
            String area_id = split[1].trim();
            String ip = split[2].trim();
            String port = split[3].trim();
            String username = split[4].trim();
            String password = split[5].trim();
            String database = split[6].trim();

            Connection conn = null;
            Statement stmt = null;
            String myDriver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://" + ip + ":" + port + "/" + database
                    + "?connectTimeout=20000&socketTimeout=20000&autoReconnect=true&failOverReadOnly=false&maxReconnects=6";

            try
            {
                Class.forName(myDriver);
                conn = DriverManager.getConnection(url, username, password);

//					System.out.println(Thread.currentThread().getName() + "连接成功:   " + poll);

                successUrl.add(poll);
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show tables like '%" + mysqlTable + "%'");

//					System.out.println(Thread.currentThread().getName() + "连接:" + poll + ": " + "show tables like '%" + mysqlTable + "%'");

                while (rs.next())
                {
                    String table = rs.getString(1);

                    if (fixTables.contains(table))
                    {
                        String sql = "select " + fields + " , " + area_id + " , " + plat + " from " + table + " where " + where;

//							System.out.println(Thread.currentThread().getName() + "连接成功并且要拉取数据的地址:   " + poll);
//							System.out.println(Thread.currentThread().getName() + "要执行的sql为： " + sql);

                        String[] cmd = new String[9];
                        cmd[0] = "sh";        // sh 或者  bash
                        cmd[1] = shellPath;    // 要执行的脚本

                        cmd[2] = ip;
                        cmd[3] = port;
                        cmd[4] = username;
                        cmd[5] = password;
                        cmd[6] = database;

                        cmd[7] = sql;            // 要执行的sql
                        // 把执行到sql的结果重定的文件名 tmp_plat_area_id_ip_database_table
                        cmd[8] = tmpDataPath + "tmp" + "_" + plat + "_" + area_id + "_" + ip + "_" + database + "_" + table + ".successed";

                        //ExecShell.callShell(cmd);
                    }
                }

                rs.close();
                stmt.close();
                conn.close();
            }
            catch (Exception e)
            {
//					e.printStackTrace();
                System.err.println("连接失败:  " + poll);
                System.out.println("连接失败的原因: " + e.getMessage());
                failUrl.add(poll);
            }
            finally
            {
                if (conn != null)
                {
                    try
                    {
                        conn.close();
                    }
                    catch (SQLException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
//			}

        }
        latch.countDown();
    }

    public static List<String> getSuccessUrl()
    {
        return successUrl;
    }

    public static List<String> getFailUrl()
    {
        return failUrl;
    }
}

