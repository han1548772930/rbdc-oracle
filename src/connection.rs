use futures_core::future::BoxFuture;
use oracle::sql_type::OracleType;
use oracle::Connection as OraConnect;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::Error;
use rbs::Value;
use std::sync::Arc;
use tokio::sync::RwLock; // 使用异步锁替代标准锁

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

// 优化：使用线程池和连接池概念
struct BlockingTask<F, R> {
    task: F,
    _phantom: std::marker::PhantomData<R>,
}

impl<F, R> BlockingTask<F, R>
where
    F: FnOnce() -> Result<R, Error> + Send + 'static,
    R: Send + 'static,
{
    fn new(task: F) -> Self {
        Self {
            task,
            _phantom: std::marker::PhantomData,
        }
    }

    async fn execute(self) -> Result<R, Error> {
        tokio::task::spawn_blocking(move || (self.task)())
            .await
            .map_err(|_| Error::from(TASK_JOIN_ERROR))?
    }
}

#[derive(Clone)]
pub struct OracleConnection {
    pub conn: Arc<OraConnect>,
    pub is_trans: Arc<RwLock<bool>>, // 使用异步读写锁
}

impl Connection for OracleConnection {
    fn get_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<Result<Vec<Box<dyn Row>>, Error>> {
        let sql: String = OracleDriver {}.pub_exchange(sql);
        let oc = self.clone();

        // 优化：预处理参数，减少在 blocking 任务中的工作
        let processed_params: Vec<_> = params.into_iter().collect();

        let task = BlockingTask::new(move || Self::execute_query(&oc.conn, &sql, processed_params));

        Box::pin(async move { task.execute().await })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<ExecResult, Error>> {
        let oc = self.clone();
        let sql = sql.to_string();

        Box::pin(async move {
            // 优化：对于简单的事务操作，避免 spawn_blocking
            match sql.as_str() {
                "begin" => {
                    let mut trans = oc.is_trans.write().await;
                    *trans = true;
                    Ok(ExecResult {
                        rows_affected: 0,
                        last_insert_id: Value::Null,
                    })
                }
                "commit" | "rollback" => {
                    let task = BlockingTask::new(move || {
                        let result = match sql.as_str() {
                            "commit" => map_oracle_error(oc.conn.commit()),
                            "rollback" => map_oracle_error(oc.conn.rollback()),
                            _ => unreachable!(),
                        };
                        result?;
                        Ok(ExecResult {
                            rows_affected: 0,
                            last_insert_id: Value::Null,
                        })
                    });

                    let result = task.execute().await?;
                    let mut trans = oc.is_trans.write().await;
                    *trans = false;
                    Ok(result)
                }
                _ => {
                    let task =
                        BlockingTask::new(move || Self::execute_statement(&oc, &sql, params));
                    task.execute().await
                }
            }
        })
    }

    fn ping(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = BlockingTask::new(move || {
            map_oracle_error(oc.conn.ping())?;
            Ok(())
        });
        Box::pin(async move { task.execute().await })
    }

    fn close(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = BlockingTask::new(move || {
            // 在关闭前确保提交所有未提交的事务
            let _ = oc.conn.commit();
            map_oracle_error(oc.conn.close())?;
            Ok(())
        });
        Box::pin(async move { task.execute().await })
    }
}

impl OracleConnection {
    pub async fn establish(opt: &OracleConnectOptions) -> Result<Self, Error> {
        let task = BlockingTask::new({
            let opt = opt.clone();
            move || {
                let conn = OraConnect::connect(opt.username, opt.password, opt.connect_string)
                    .map_err(|e| Error::from(e.to_string()))?;

                Ok(OracleConnection {
                    conn: Arc::new(conn),
                    is_trans: Arc::new(RwLock::new(false)),
                })
            }
        });

        task.execute().await
    }

    // 优化：将复杂逻辑提取到单独的方法中，减少闭包大小
    fn execute_query(
        conn: &Arc<OraConnect>,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<Box<dyn Row>>, Error> {
        let builder = conn.statement(sql);
        let mut stmt = map_oracle_error(builder.build())?;

        for (idx, x) in params.into_iter().enumerate() {
            x.encode(idx, &mut stmt)?;
        }

        let rows = map_oracle_error(stmt.query(&[]))?;
        let col_infos = rows.column_info();
        let col_count = col_infos.len();

        // 优化：使用 with_capacity 预分配
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
                let oracle_data = Self::process_column_data(col, t.clone())?;
                datas.push(oracle_data);
            }

            let row = OracleRow {
                columns: columns_arc.clone(),
                datas,
            };
            results.push(Box::new(row) as Box<dyn Row>);
        }
        Ok(results)
    }

    fn execute_statement(
        oc: &OracleConnection,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<ExecResult, Error> {
        // 只在需要时才进行 SQL 转换
        let processed_sql = if sql.contains('?') {
            OracleDriver {}.pub_exchange(sql)
        } else {
            sql.to_string()
        };

        let builder = oc.conn.statement(&processed_sql);
        let mut stmt = map_oracle_error(builder.build())?;

        if !params.is_empty() {
            for (idx, x) in params.into_iter().enumerate() {
                x.encode(idx, &mut stmt)?;
            }
        }

        map_oracle_error(stmt.execute(&[]))?;

        // 异步事务检查优化
        let should_commit = match oc.is_trans.try_read() {
            Ok(trans) => !*trans,
            Err(_) => false,
        };

        if should_commit {
            map_oracle_error(oc.conn.commit())?;
        }

        let rows_affected = map_oracle_error(stmt.row_count())?;

        Ok(ExecResult {
            rows_affected,
            last_insert_id: Value::Null,
        })
    }

    // 优化：提取列数据处理逻辑
    fn process_column_data(col: &oracle::SqlValue, t: OracleType) -> Result<OracleData, Error> {
        if let Ok(true) = col.is_null() {
            return Ok(OracleData {
                str: None,
                bin: None,
                column_type: t,
                is_sql_null: true,
            });
        }

        if matches!(t, OracleType::BLOB) {
            match col.get::<Vec<u8>>() {
                Ok(bin) => Ok(OracleData {
                    str: None,
                    bin: Some(bin.into()),
                    column_type: t,
                    is_sql_null: false,
                }),
                Err(_) => Ok(OracleData {
                    str: None,
                    bin: None,
                    column_type: t,
                    is_sql_null: false,
                }),
            }
        } else {
            match col.get::<String>() {
                Ok(str_val) => Ok(OracleData {
                    str: Some(str_val.into()),
                    bin: None,
                    column_type: t,
                    is_sql_null: false,
                }),
                Err(_) => Ok(OracleData {
                    str: None,
                    bin: None,
                    column_type: t,
                    is_sql_null: false,
                }),
            }
        }
    }
}
