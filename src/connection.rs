use std::sync::{Arc, Mutex};
use futures_core::future::BoxFuture;
use oracle::sql_type::OracleType;
use oracle::Connection as OraConnect;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::Error;
use rbs::Value;

use crate::driver::OracleDriver;
use crate::encode::Encode;
use crate::options::OracleConnectOptions;
use crate::{OracleColumn, OracleData, OracleRow};

// 定义常量错误消息，避免重复分配
const TASK_JOIN_ERROR: &str = "Task join error";

// 创建一个帮助函数来统一错误处理
#[inline]
fn map_oracle_error<T>(result: Result<T, oracle::Error>) -> Result<T, Error> {
    result.map_err(|e| Error::from(e.to_string()))
}

#[derive(Clone)]
pub struct OracleConnection {
    pub conn: Arc<OraConnect>,
    pub is_trans: Arc<Mutex<bool>>,
}

impl Connection for OracleConnection {
    fn get_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<Result<Vec<Box<dyn Row>>, Error>> {
        let sql: String = OracleDriver {}.pub_exchange(sql);
        let oc = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            let builder = oc.conn.statement(&sql);
            let mut stmt = map_oracle_error(builder.build())?;

            for (idx, x) in params.into_iter().enumerate() {
                x.encode(idx, &mut stmt)?;
            }

            let rows = map_oracle_error(stmt.query(&[]))?;
            let col_infos = rows.column_info();
            let col_count = col_infos.len();

            // 预分配容量
            let mut results = Vec::new();
            let mut columns = Vec::with_capacity(col_count);

            for info in col_infos.iter() {
                columns.push(OracleColumn {
                    name: info.name().to_lowercase().into(),
                    column_type: info.oracle_type().clone(),
                });
            }

            let columns_arc = Arc::new(columns);

            for row_result in rows {
                let row = map_oracle_error(row_result)?;
                let mut datas = Vec::with_capacity(col_count);

                for col in row.sql_values().iter() {
                    let t = map_oracle_error(col.oracle_type())?;

                    let oracle_data = if let Ok(true) = col.is_null() {
                        OracleData {
                            str: None,
                            bin: None,
                            column_type: t.clone(),
                            is_sql_null: true,
                        }
                    } else {
                        // 使用 matches! 宏进行类型匹配
                        if matches!(t, OracleType::BLOB) {
                            match col.get::<Vec<u8>>() {
                                Ok(bin) => OracleData {
                                    str: None,
                                    bin: Some(bin.into()),
                                    column_type: t.clone(),
                                    is_sql_null: false,
                                },
                                Err(_) => OracleData {
                                    str: None,
                                    bin: None,
                                    column_type: t.clone(),
                                    is_sql_null: false,
                                },
                            }
                        } else {
                            match col.get::<String>() {
                                Ok(str_val) => OracleData {
                                    str: Some(str_val.into()),
                                    bin: None,
                                    column_type: t.clone(),
                                    is_sql_null: false,
                                },
                                Err(_) => OracleData {
                                    str: None,
                                    bin: None,
                                    column_type: t.clone(),
                                    is_sql_null: false,
                                },
                            }
                        }
                    };
                    datas.push(oracle_data);
                }

                let row = OracleRow {
                    columns: columns_arc.clone(),
                    datas,
                };
                results.push(Box::new(row) as Box<dyn Row>);
            }
            Ok(results)
        });

        Box::pin(async move { task.await.map_err(|_| Error::from(TASK_JOIN_ERROR))? })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<ExecResult, Error>> {
        let oc = self.clone();
        let sql = sql.to_string();
        let task = tokio::task::spawn_blocking(move || {
            let mut trans = oc.is_trans.lock().map_err(|e| Error::from(e.to_string()))?;

            match sql.as_str() {
                "begin" => {
                    *trans = true;
                    Ok(ExecResult {
                        rows_affected: 0,
                        last_insert_id: Value::Null,
                    })
                }
                "commit" => {
                    map_oracle_error(oc.conn.commit())?;
                    *trans = false;
                    Ok(ExecResult {
                        rows_affected: 0,
                        last_insert_id: Value::Null,
                    })
                }
                "rollback" => {
                    map_oracle_error(oc.conn.rollback())?;
                    *trans = false;
                    Ok(ExecResult {
                        rows_affected: 0,
                        last_insert_id: Value::Null,
                    })
                }
                _ => {
                    let processed_sql = OracleDriver {}.pub_exchange(&sql);
                    let builder = oc.conn.statement(&processed_sql);
                    let mut stmt = map_oracle_error(builder.build())?;

                    for (idx, x) in params.into_iter().enumerate() {
                        x.encode(idx, &mut stmt)?;
                    }

                    map_oracle_error(stmt.execute(&[]))?;

                    if !*trans {
                        map_oracle_error(oc.conn.commit())?;
                    }

                    let rows_affected = map_oracle_error(stmt.row_count())?;

                    // 简化 last_insert_id 处理，避免不稳定的 bind_value 调用
                    Ok(ExecResult {
                        rows_affected,
                        last_insert_id: Value::Null,
                    })
                }
            }
        });

        Box::pin(async move { task.await.map_err(|_| Error::from(TASK_JOIN_ERROR))? })
    }

    fn ping(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            map_oracle_error(oc.conn.ping())?;
            Ok(())
        });
        Box::pin(async { task.await.map_err(|_| Error::from(TASK_JOIN_ERROR))? })
    }

    fn close(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            // 在关闭前确保提交所有未提交的事务
            let _ = oc.conn.commit();
            map_oracle_error(oc.conn.close())?;
            Ok(())
        });
        Box::pin(async { task.await.map_err(|_| Error::from(TASK_JOIN_ERROR))? })
    }
}

impl OracleConnection {
    pub async fn establish(opt: &OracleConnectOptions) -> Result<Self, Error> {
        let task = tokio::task::spawn_blocking({
            let opt = opt.clone();
            move || {
                let conn = OraConnect::connect(
                    opt.username,
                    opt.password,
                    opt.connect_string,
                ).map_err(|e| Error::from(e.to_string()))?;
                
                Ok(OracleConnection {
                    conn: Arc::new(conn),
                    is_trans: Arc::new(Mutex::new(false)),
                })
            }
        });
        
        task.await.map_err(|_| Error::from(TASK_JOIN_ERROR))?
    }
}