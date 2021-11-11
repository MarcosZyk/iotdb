package org.apache.iotdb.db.query.control;

import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.apache.thrift.async.AsyncMethodCallback;

public class ExecuteQueryHandler implements AsyncMethodCallback<TSExecuteStatementResp> {

  long statementId;
  TSExecuteStatementResp result;

  public void setStatementId(long statementId) {
    this.statementId = statementId;
  }

  public TSExecuteStatementResp getResult() {
    return result;
  }

  @Override
  public void onComplete(TSExecuteStatementResp tsExecuteStatementResp) {
    this.result = tsExecuteStatementResp;
  }

  @Override
  public void onError(Exception e) {}
}
