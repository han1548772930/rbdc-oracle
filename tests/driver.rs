#[cfg(test)]
mod test {
    use rbdc::db::Placeholder;
    use rbdc_oracle::driver::OracleDriver;

    #[test]
    fn test_exchange() {
        let v = "insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        let d = OracleDriver {};
        let sql = d.exchange(v);
        assert_eq!("insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)", sql);
    }

    #[test]
    fn test_exchange_select() {
        let d = OracleDriver {};
        let sql = d.exchange("SELECT * FROM users WHERE id = ? AND name = ?");
        assert_eq!("SELECT * FROM users WHERE id = :1 AND name = :2", sql);
    }

    #[test]
    fn test_exchange_no_params() {
        let d = OracleDriver {};
        let sql = d.exchange("SELECT * FROM users");
        assert_eq!("SELECT * FROM users", sql);
    }

    #[test]
    fn test_exchange_complex() {
        let d = OracleDriver {};
        let sql = d.exchange("UPDATE table SET col1 = ?, col2 = ? WHERE id IN (?, ?, ?)");
        assert_eq!(
            "UPDATE table SET col1 = :1, col2 = :2 WHERE id IN (:3, :4, :5)",
            sql
        );
    }
}

// 更新测试代码
#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod integration_tests {
    use rbdc::db::Connection; // 添加这个导入
    use rbdc_oracle::{OracleConnectOptions, OracleConnection};
    use rbs::Value;

    async fn get_test_connection() -> OracleConnection {
        let connection_string = "//10.66.66.250:1521/HDDZ/";

        let username = "HDDZ";
        let password = "HDDZ";

        println!("Connecting to Oracle: {}@{}", username, connection_string);

        let opts = OracleConnectOptions::new(&username, &password, &connection_string);
        OracleConnection::establish(&opts)
            .await
            .expect("Failed to connect to Oracle")
    }

    #[tokio::test]
    async fn test_simple_query() {
        let mut conn = get_test_connection().await;

        // 测试简单查询
        let rows = conn
            .get_rows("select get_sysid('mes_map_detail') id from dual", vec![])
            .await
            .expect("Query failed");

        assert_eq!(rows.len(), 1);

        // 获取具体的查询结果
        if let Some(mut row) = rows.into_iter().next() {
            // 获取第一列的值 (索引为0)
            let id_value = row.get(0).expect("Failed to get column value");
            println!("查询结果: {:?}", id_value);

            // 根据 rbs::Value 的类型进行处理
            match id_value {
                rbs::Value::I32(val) => println!("ID 是 i32: {}", val),
                rbs::Value::I64(val) => println!("ID 是 i64: {}", val),
                rbs::Value::String(val) => println!("ID 是字符串: {}", val),
                _ => println!("其他类型: {:?}", id_value),
            }
        }

        println!("✅ Simple query test successful!");
    }
}
