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
        assert_eq!("UPDATE table SET col1 = :1, col2 = :2 WHERE id IN (:3, :4, :5)", sql);
    }
}

// 集成测试（需要数据库连接）
#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod integration_tests {
    use rbdc_oracle::{OracleConnectOptions, OracleConnection};
    use std::env;

    async fn get_test_connection() -> OracleConnection {
        let connection_string = env::var("ORACLE_CONNECTION_STRING")
            .unwrap_or_else(|_| "//localhost:1521/XE".to_string());
        let username = env::var("ORACLE_USERNAME")
            .unwrap_or_else(|_| "testuser".to_string());
        let password = env::var("ORACLE_PASSWORD")
            .unwrap_or_else(|_| "testpass".to_string());

        let opts = OracleConnectOptions::new(&username, &password, &connection_string);
        OracleConnection::establish(&opts).await.expect("Failed to connect to Oracle")
    }

    #[tokio::test]
    async fn test_connection() {
        let _conn = get_test_connection().await;
        // 如果能建立连接就算成功
    }

    #[tokio::test]
    async fn test_ping() {
        let mut conn = get_test_connection().await;
        conn.ping().await.expect("Ping failed");
    }
}