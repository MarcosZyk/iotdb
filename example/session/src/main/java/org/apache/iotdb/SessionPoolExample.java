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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SessionPoolExample {

  private static int TOTAL_DEVICE_NUM = 18000000;

  private static final int LEVEL_NUM = 8;

  private static final int RANDOM_STR_LEN = 32;

  private static final int GROUP_NUM = 12;

  private static final int DEVICE_NUM = 2500;

  private static final int DEVICE_GROUP_NUM = TOTAL_DEVICE_NUM / GROUP_NUM / DEVICE_NUM;

  private static final List<String> MEASUREMENT = new ArrayList<>(Collections.singleton("field"));

  private static final List<TSDataType> TYPE =
      new ArrayList<>(Collections.singleton(TSDataType.DOUBLE));

  private static final List<Object> VALUE = new ArrayList<>(Collections.singleton(1.0));

  private static List<String> storageGroupList;

  private static final Map<String, List<String>> sgDeviceList = new HashMap<>();

  private static final Map<String, Session> sgSessionMap = new HashMap<>();

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    if (args.length != 0) {
      TOTAL_DEVICE_NUM = Integer.parseInt(args[0]);
    }
    generateStorageGroup();
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    for (int i = 0; i < GROUP_NUM; i++) {
      session.setStorageGroup("root." + storageGroupList.get(i));
    }
    Map<String, EndPoint> sgEndPointMap = session.getStorageGroupDistribution();
    for (Map.Entry<String, EndPoint> stringEndPointEntry : sgEndPointMap.entrySet()) {
      EndPoint endPoint = stringEndPointEntry.getValue();
      Session endPointSession = new Session(endPoint.ip, endPoint.port, "root", "root");
      endPointSession.open();
      sgSessionMap.put(stringEndPointEntry.getKey(), endPointSession);
    }
    generateDeviceIds();
    for (Map.Entry<String, List<String>> sgDeviceEntry : sgDeviceList.entrySet()) {
      (new Thread(new WriteThread(sgDeviceEntry.getKey(), sgDeviceEntry.getValue()))).start();
    }
  }

  private static void generateStorageGroup() {
    storageGroupList = new ArrayList<>();
    for (int i = 0; i < GROUP_NUM; i++) {
      String randomStr = getRandomStr() + "_" + i;
      storageGroupList.add(randomStr);
    }
  }

  private static void generateDeviceIds() {
    for (int i = 0; i < DEVICE_GROUP_NUM; i++) {
      List<String> levels = new ArrayList<>();
      levels.add(getRandomStr());
      for (int j = 0; j < DEVICE_NUM; j++) {
        for (int k = 0; k < LEVEL_NUM - 2; k++) {
          levels.add(getRandomStr());
        }
        String deviceId = StringUtils.join(levels, ".");
        for (int k = 0; k < GROUP_NUM; k++) {
          String storageGroup = "root." + storageGroupList.get(k);
          List<String> deviceIds = sgDeviceList.computeIfAbsent(storageGroup, t -> new ArrayList());
          deviceIds.add(storageGroup + "." + deviceId);
        }
      }
    }
  }

  static class WriteThread implements Runnable {

    String sg;

    List<String> devices;

    public WriteThread(String sg, List<String> devices) {
      this.sg = sg;
      this.devices = devices;
    }

    public void run() {
      while (true) {
        try {
          insertRecords();
        } catch (StatementExecutionException | IoTDBConnectionException e) {
          e.printStackTrace();
        }
      }
    }

    private void insertRecords() throws StatementExecutionException, IoTDBConnectionException {
      long time = System.currentTimeMillis();
      Session session = SessionPoolExample.sgSessionMap.get(this.sg);
      List<Long> times = new ArrayList<>();
      List<List<String>> measurementsList = new ArrayList<>();
      List<List<Object>> valuesList = new ArrayList<>();
      List<List<TSDataType>> typesList = new ArrayList<>();
      for (int j = 0; j < this.devices.size(); j++) {
        times.add(time);
        measurementsList.add(SessionPoolExample.MEASUREMENT);
        typesList.add(SessionPoolExample.TYPE);
        valuesList.add(SessionPoolExample.VALUE);
      }
      session.insertRecords(this.devices, times, measurementsList, typesList, valuesList);
      System.out.printf("already insert sg:%s for %d devices%n", this.sg, this.devices.size());
    }
  }

  public static String getRandomStr() {
    String str = "qwertyuiopasdfghjklzxcvbnm";
    Random random = new Random();
    StringBuilder stringBuffer = new StringBuilder();
    for (int i = 0; i < RANDOM_STR_LEN; i++) {
      int index = random.nextInt(str.length());
      char c = str.charAt(index);
      stringBuffer.append(c);
    }
    return stringBuffer.toString();
  }
}
