use crate::connection::OracleConnection;
use crate::options::OracleConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection};
use rbdc::db::{Driver, Placeholder};
use rbdc::{impl_exchange, Error};

#[derive(Debug)]
pub struct OracleDriver {}

impl Driver for OracleDriver {
    fn name(&self) -> &str {
        "oracle"
    }

    fn connect(&self, url: &str) -> BoxFuture<Result<Box<dyn Connection>, Error>> {
        // 修复：克隆 url 字符串以避免生命周期问题
        let url = url.to_string();

        Box::pin(async move {
            // 解析 URL: oracle://username:password@host:port/service
            let parsed_url =
                url::Url::parse(&url).map_err(|e| Error::from(format!("Invalid URL: {}", e)))?;

            if parsed_url.scheme() != "oracle" {
                return Err(Error::from("URL scheme must be 'oracle'"));
            }

            let username = parsed_url.username().to_string();
            let password = parsed_url
                .password()
                .ok_or_else(|| Error::from("Password is required"))?
                .to_string();

            let host = parsed_url
                .host_str()
                .ok_or_else(|| Error::from("Host is required"))?;
            let port = parsed_url.port().unwrap_or(1521);
            let service = parsed_url.path().trim_start_matches('/');

            let connect_string = if service.is_empty() {
                format!("//{}:{}", host, port)
            } else {
                format!("//{}:{}/{}", host, port, service)
            };

            let opt = OracleConnectOptions::new(&username, &password, &connect_string);
            let conn = OracleConnection::establish(&opt).await?;
            Ok(Box::new(conn) as Box<dyn Connection>)
        })
    }

    fn connect_opt(
        &self,
        opt: &dyn ConnectOptions,
    ) -> BoxFuture<Result<Box<dyn Connection>, Error>> {
        // 修复：克隆选项以避免生命周期问题
        let opt = match opt.downcast_ref::<OracleConnectOptions>() {
            Some(oracle_opt) => oracle_opt.clone(),
            None => return Box::pin(async { Err(Error::from("Invalid connection options type")) }),
        };

        Box::pin(async move {
            let conn = OracleConnection::establish(&opt).await?;
            Ok(Box::new(conn) as Box<dyn Connection>)
        })
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(OracleConnectOptions::default())
    }
}

impl Placeholder for OracleDriver {
    fn exchange(&self, sql: &str) -> String {
        impl_exchange(":", 1, sql)
    }
}

impl OracleDriver {
    pub fn pub_exchange(&self, sql: &str) -> String {
        self.exchange(sql)
    }
}
