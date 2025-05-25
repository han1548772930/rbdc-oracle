use crate::decode::Decode;
use oracle::sql_type::OracleType;
use rbdc::db::{MetaData, Row};
use rbs::Value;
use std::sync::Arc;

pub mod connection;
pub mod decode;
pub mod driver;
pub mod encode;
pub mod options;

#[derive(Debug, Clone)]
pub struct OracleColumn {
    pub name: Arc<str>,
    pub column_type: OracleType,
}

#[derive(Debug)]
pub struct OracleMetaData(pub Arc<Vec<OracleColumn>>);

impl MetaData for OracleMetaData {
    fn column_len(&self) -> usize {
        self.0.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.0[i].name.to_string()
    }

    fn column_type(&self, i: usize) -> String {
        format!("{:?}", self.0[i].column_type)
    }
}

#[derive(Debug)]
pub struct OracleData {
    pub str: Option<Arc<str>>,  // 使用 Arc<str> 减少内存占用
    pub bin: Option<Arc<[u8]>>, // 使用 Arc<[u8]> 减少内存占用
    pub column_type: OracleType,
    pub is_sql_null: bool,
}

#[derive(Debug)]
pub struct OracleRow {
    pub columns: Arc<Vec<OracleColumn>>,
    pub datas: Vec<OracleData>,
}

impl Row for OracleRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(OracleMetaData(self.columns.clone()))
    }

    fn get(&mut self, i: usize) -> Result<Value, rbdc::Error> {
        self.get_safe(i)
    }
}

impl OracleRow {
    #[inline]
    pub fn get_safe(&self, i: usize) -> Result<Value, rbdc::Error> {
        self.datas
            .get(i)
            .ok_or_else(|| rbdc::Error::from("Index out of bounds"))
            .and_then(Value::decode)
    }
}
