use std::str::FromStr;

use bigdecimal::BigDecimal;
use oracle::Statement;
use rbdc::Error;
use rbs::Value;

pub trait Encode {
    fn encode(self, idx: usize, statement: &mut Statement) -> Result<(), Error>;
}

impl Encode for Value {
    fn encode(self, idx: usize, statement: &mut Statement) -> Result<(), Error> {
        let idx = idx + 1; // Oracle 是基于 1 的索引

        match self {
            Value::Ext(t, v) => match t {
                "Date" => {
                    // 修复：使用 as_string() 而不是 as_str()
                    let date_str = v.as_string().unwrap_or_default();
                    let date = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                        .map_err(|e| Error::from(e.to_string()))?;
                    statement
                        .bind(idx, &date.to_string())
                        .map_err(|e| Error::from(e.to_string()))
                }
                "DateTime" => {
                    let datetime_str = v.as_string().unwrap_or_default();
                    let datetime =
                        chrono::NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%dT%H:%M:%S")
                            .map_err(|e| Error::from(e.to_string()))?;
                    statement
                        .bind(idx, &datetime.to_string())
                        .map_err(|e| Error::from(e.to_string()))
                }
                "Time" => {
                    let time_str = v.as_string().unwrap_or_default();
                    statement
                        .bind(idx, &time_str)
                        .map_err(|e| Error::from(e.to_string()))
                }
                "Decimal" => {
                    let decimal_str = v.as_string().unwrap_or_default();
                    let decimal = BigDecimal::from_str(&decimal_str)
                        .map_err(|e| Error::from(e.to_string()))?;
                    statement
                        .bind(idx, &decimal.to_string())
                        .map_err(|e| Error::from(e.to_string()))
                }
                "Timestamp" => {
                    let timestamp = v.as_u64().unwrap_or_default() as i64;
                    statement
                        .bind(idx, &timestamp)
                        .map_err(|e| Error::from(e.to_string()))
                }
                "Uuid" => {
                    let uuid_str = v.as_string().unwrap_or_default();
                    statement
                        .bind(idx, &uuid_str)
                        .map_err(|e| Error::from(e.to_string()))
                }
                "Json" => Err(Error::from("JSON type not implemented")),
                _ => Err(Error::from("Unknown extended type")),
            },
            Value::String(s) => statement
                .bind(idx, &s)
                .map_err(|e| Error::from(e.to_string())),
            Value::U32(u) => statement
                .bind(idx, &u)
                .map_err(|e| Error::from(e.to_string())),
            Value::U64(u) => statement
                .bind(idx, &u)
                .map_err(|e| Error::from(e.to_string())),
            Value::I32(i) => statement
                .bind(idx, &i)
                .map_err(|e| Error::from(e.to_string())),
            Value::I64(i) => statement
                .bind(idx, &i)
                .map_err(|e| Error::from(e.to_string())),
            Value::F32(f) => statement
                .bind(idx, &f)
                .map_err(|e| Error::from(e.to_string())),
            Value::F64(f) => statement
                .bind(idx, &f)
                .map_err(|e| Error::from(e.to_string())),
            Value::Binary(bin) => statement
                .bind(idx, &bin)
                .map_err(|e| Error::from(e.to_string())),
            Value::Null => {
                // 修复：使用 Option<String> 而不是 Option<&str>
                let null_val: Option<String> = None;
                statement
                    .bind(idx, &null_val)
                    .map_err(|e| Error::from(e.to_string()))
            }
            Value::Bool(b) => {
                // 将布尔值转换为整数
                let val = if b { 1i32 } else { 0i32 };
                statement
                    .bind(idx, &val)
                    .map_err(|e| Error::from(e.to_string()))
            }
            Value::Array(_) => {
                // 数组类型暂不支持，转换为字符串
                let str_val = self.to_string();
                statement
                    .bind(idx, &str_val)
                    .map_err(|e| Error::from(e.to_string()))
            }
            _ => {
                let str_val = self.to_string();
                statement
                    .bind(idx, &str_val)
                    .map_err(|e| Error::from(e.to_string()))
            }
        }
    }
}
