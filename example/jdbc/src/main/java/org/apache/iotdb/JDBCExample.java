/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JDBCExample {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try {
      int threadNum = 50;
      ExecutorService service = Executors.newFixedThreadPool(threadNum);

      String sql =
          "select count(*) from root.*.muooryhednieihefozlktytyqshsfqtc group by ([now()-12h, now()-11h), 1m), level=2";
      for (int i = 0; i < threadNum; i++) {
        service.submit(() -> query(sql));
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  private static void query(String sql) {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://172.20.70.55:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      long totalTime = 0;
      int executeNum = 1;
      for (int i = 0; i < executeNum; i++) {
        long startTime = System.currentTimeMillis();
        statement.execute(sql);
        long endTime = System.currentTimeMillis();
        totalTime += endTime - startTime;
      }
      System.out.println("Avg Cost: " + totalTime / executeNum);
    } catch (Exception e) {
      System.out.println("出错啦" + e.getMessage());
    }
  }

  private static void outputResult(ResultSet resultSet) throws SQLException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }

  private static String prepareInsertStatment(int time) {
    return "insert into root.sg1.d1(timestamp, s1, s2, s3) values("
        + time
        + ","
        + 1
        + ","
        + 1
        + ","
        + 1
        + ")";
  }
}
