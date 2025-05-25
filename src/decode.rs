use bigdecimal::BigDecimal;
use oracle::sql_type::OracleType;
use rbdc::{datetime::DateTime, Error};
use rbs::Value;
use std::{str::FromStr, sync::OnceLock};

use crate::OracleData;

pub trait Decode {
    fn decode(row: &OracleData) -> Result<Value, Error>;
}
// 使用静态常量避免重复分配
static DECIMAL_EXT: OnceLock<String> = OnceLock::new();
const MISSING_STRING_VALUE: &str = "Missing string value";

impl Decode for Value {
    fn decode(row: &OracleData) -> Result<Value, Error> {
        if row.is_sql_null {
            return Ok(Value::Null);
        }

        match row.column_type {
            OracleType::Number(p, s) => {
                let value = row
                    .str
                    .as_ref()
                    .ok_or_else(|| Error::from(MISSING_STRING_VALUE))?;

                if p == 0 && s == -127 {
                    // number(*) 类型处理
                    let dec =
                        BigDecimal::from_str(value).map_err(|e| Error::from(e.to_string()))?;

                    if dec.is_integer() {
                        let digits = dec.digits();
                        return match digits {
                            1..=9 => value
                                .parse::<i32>()
                                .map(Value::I32)
                                .map_err(|e| Error::from(e.to_string())),
                            10..=18 => value
                                .parse::<i64>()
                                .map(Value::I64)
                                .map_err(|e| Error::from(e.to_string())),
                            _ => {
                                let decimal_ext = DECIMAL_EXT.get_or_init(|| "Decimal".to_string());
                                Ok(Value::String(dec.to_string()).into_ext(decimal_ext))
                            }
                        };
                    }
                    let decimal_ext = DECIMAL_EXT.get_or_init(|| "Decimal".to_string());
                    return Ok(Value::String(dec.to_string()).into_ext(decimal_ext));
                }

                if s > 0 {
                    let dec =
                        BigDecimal::from_str(value).map_err(|e| Error::from(e.to_string()))?;
                    let decimal_ext = DECIMAL_EXT.get_or_init(|| "Decimal".to_string());
                    return Ok(Value::String(dec.to_string()).into_ext(decimal_ext));
                }

                // 整数类型处理
                match p {
                    1..=9 => value
                        .parse::<i32>()
                        .map(Value::I32)
                        .map_err(|e| Error::from(e.to_string())),
                    10..=18 => value
                        .parse::<i64>()
                        .map(Value::I64)
                        .map_err(|e| Error::from(e.to_string())),
                    _ => {
                        let dec =
                            BigDecimal::from_str(value).map_err(|e| Error::from(e.to_string()))?;
                        let decimal_ext = DECIMAL_EXT.get_or_init(|| "Decimal".to_string());
                        Ok(Value::String(dec.to_string()).into_ext(decimal_ext))
                    }
                }
            }
            OracleType::Int64 => {
                let value = row
                    .str
                    .as_ref()
                    .ok_or_else(|| Error::from(MISSING_STRING_VALUE))?;
                value
                    .parse::<i64>()
                    .map(Value::I64)
                    .map_err(|e| Error::from(e.to_string()))
            }
            OracleType::Float(p) => {
                let value = row
                    .str
                    .as_ref()
                    .ok_or_else(|| Error::from(MISSING_STRING_VALUE))?;
                if p >= 24 {
                    value
                        .parse::<f64>()
                        .map(Value::F64)
                        .map_err(|e| Error::from(e.to_string()))
                } else {
                    value
                        .parse::<f32>()
                        .map(Value::F32)
                        .map_err(|e| Error::from(e.to_string()))
                }
            }
            OracleType::Date => {
                let value = row
                    .str
                    .as_ref()
                    .ok_or_else(|| Error::from(MISSING_STRING_VALUE))?;
                DateTime::from_str(value)
                    .map(Value::from)
                    .map_err(|e| Error::from(e.to_string()))
            }
            OracleType::BLOB => Ok(row
                .bin
                .as_ref()
                .map(|bin| Value::Binary((**bin).to_vec()))
                .unwrap_or(Value::Null)),
            OracleType::Long | OracleType::CLOB | OracleType::NCLOB => {
                let value = row
                    .str
                    .as_ref()
                    .ok_or_else(|| Error::from(MISSING_STRING_VALUE))?;
                Ok(Value::String((**value).to_string()))
            }
            _ => row
                .str
                .as_ref()
                .map(|s| Value::String((**s).to_string()))
                .ok_or_else(|| Error::from("unimpl")),
        }
    }
}
