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
            let mut stmt = builder.build().map_err(|e| Error::from(e.to_string()))?;

            for (idx, x) in params.into_iter().enumerate() {
                x.encode(idx, &mut stmt)
                    .map_err(|e| Error::from(e.to_string()))?
            }

            let rows = stmt.query(&[]).map_err(|e| Error::from(e.to_string()))?;
            let col_infos = rows.column_info();
            let col_count = col_infos.len();
            let mut results = Vec::new();
            let mut columns = Vec::with_capacity(col_count);

            // 预先构建列类型映射，避免在循环中重复clone
            let mut column_types = Vec::with_capacity(col_count);
            for info in col_infos.iter() {
                let oracle_type = info.oracle_type().clone();
                columns.push(OracleColumn {
                    name: info.name().to_string().to_lowercase(),
                    column_type: oracle_type.clone(),
                });
                column_types.push(oracle_type);
            }

            let columns_arc = Arc::new(columns);

            for row_result in rows {
                let row = row_result.map_err(|e| Error::from(e.to_string()))?;
                let mut datas = Vec::with_capacity(col_count);

                for (col_idx, col) in row.sql_values().iter().enumerate() {
                    // 直接使用预先获取的类型，避免clone
                    let t = &column_types[col_idx];

                    let oracle_data = if let Ok(true) = col.is_null() {
                        OracleData {
                            str: None,
                            bin: None,
                            column_type: t.clone(), // 只在这里clone一次
                            is_sql_null: true,
                        }
                    } else if *t == OracleType::BLOB {
                        let bin = col.get::<Vec<u8>>().ok();
                        OracleData {
                            str: None,
                            bin,
                            column_type: t.clone(),
                            is_sql_null: false,
                        }
                    } else {
                        let str_val = col.get::<String>().ok();
                        OracleData {
                            str: str_val,
                            bin: None,
                            column_type: t.clone(),
                            is_sql_null: false,
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
        Box::pin(async move { task.await.map_err(|e| Error::from(e.to_string()))? })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<ExecResult, Error>> {
        let oc = self.clone();
        let sql = sql.to_string();
        let task = tokio::task::spawn_blocking(move || {
            let mut trans = oc.is_trans.lock().map_err(|e| Error::from(e.to_string()))?;
            if sql == "begin" {
                *trans = true;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else if sql == "commit" {
                oc.conn.commit().unwrap();
                *trans = false;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else if sql == "rollback" {
                oc.conn.rollback().unwrap();
                *trans = false;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else {
                let sql: String = OracleDriver {}.pub_exchange(&sql);
                let builder = oc.conn.statement(&sql);
                let mut stmt = builder.build().map_err(|e| Error::from(e.to_string()))?;
                for (idx, x) in params.into_iter().enumerate() {
                    x.encode(idx, &mut stmt)
                        .map_err(|e| Error::from(e.to_string()))?
                }
                stmt.execute(&[]).map_err(|e| Error::from(e.to_string()))?;
                if !*trans {
                    oc.conn.commit().map_err(|e| Error::from(e.to_string()))?;
                    *trans = false;
                }
                let rows_affected = stmt.row_count().map_err(|e| Error::from(e.to_string()))?;
                let mut ret = vec![];
                for i in 1..=stmt.bind_count() {
                    let res: Result<String, _> = stmt.bind_value(i);
                    match res {
                        Ok(v) => ret.push(Value::String(v)),
                        Err(_) => ret.push(Value::Null),
                    }
                }
                Ok(ExecResult {
                    rows_affected,
                    last_insert_id: Value::Array(ret),
                })
            }
        });
        Box::pin(async { task.await.map_err(|e| Error::from(e.to_string()))? })
    }

    fn ping(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            oc.conn.ping().map_err(|e| Error::from(e.to_string()))?;
            Ok(())
        });
        Box::pin(async { task.await.map_err(|e| Error::from(e.to_string()))? })
    }

    fn close(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            oc.conn.commit().map_err(|e| Error::from(e.to_string()))?;
            oc.conn.close().map_err(|e| Error::from(e.to_string()))?;
            Ok(())
        });
        Box::pin(async { task.await.map_err(|e| Error::from(e.to_string()))? })
    }
}

impl OracleConnection {
    pub async fn establish(opt: &OracleConnectOptions) -> Result<Self, Error> {
        let conn = OraConnect::connect(
            opt.username.clone(),
            opt.password.clone(),
            opt.connect_string.clone(),
        )
        .map_err(|e| Error::from(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(conn),
            is_trans: Arc::new(Mutex::new(false)),
        })
    }
}
