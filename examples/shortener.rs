use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{header::LOCATION, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt::Layer, layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

#[derive(Debug, Error)]
enum ShortenError {
    #[error("Connect Database Error:{0}")]
    Database(String),
    #[error("Sqlx Error:{0}")]
    SqlxQuery(#[from] sqlx::Error),
    #[error("Url parse Error:{0}")]
    UrlParse(String),
}
#[derive(Debug, Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Debug, Deserialize)]
struct ShortenReq {
    url: String,
}

#[derive(Debug, Serialize)]
struct ShortenRes {
    location: String,
}

// Urls解构数据返回行Row，所以要派生sqlx的FromRow，并且为空时返回字段默认值
#[derive(Debug, FromRow)]
struct Urls {
    #[sqlx(default)]
    id: String,
    #[sqlx(default)]
    url: String,
}

const ADDR: &str = "127.0.0.1:8080";
#[tokio::main]
async fn main() -> Result<()> {
    let layer = Layer::new().with_filter(LevelFilter::INFO);
    tracing_subscriber::registry().with(layer).init();

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    // 配置postgres数据源地址，用sqlx的postgres驱动创建连接池
    let url = "postgres://postgres:123456@localhost:5432/shortener";
    let state = AppState::try_new(url).await?;
    info!("Connected to database:{}", url);

    // 注册路由
    let router = Router::new()
        .route("/", post(shorten))
        .route("/:id", get(redirect))
        .with_state(state);

    // 注册监听器和路由器，并启动web服务器
    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}

async fn shorten(
    State(state): State<AppState>,
    Json(body): Json<ShortenReq>,
) -> Result<impl IntoResponse, StatusCode> {
    // Json Body Extractor提取器，按json格式提取body，获取body中的url字段
    let url = body.url;
    // 将url添加到数据库中
    let id = state.add(url).await.map_err(|e| {
        warn!("Database add shorten error:{}", e);
        StatusCode::UNPROCESSABLE_ENTITY
    })?;

    // 将返回封装成一个ShortenRes对象，再转Json格式
    let body = Json(ShortenRes {
        location: format!("http://{}/{}", ADDR, id),
    });

    // 返回状态码+body
    Ok((StatusCode::CREATED, body))
}

// 根据短url，返回一个重定向响应response。以 HTTP/1.1 308 OK Location:https://baidu.com 返回
// 浏览器自动重新发起一次请求，访问指定的url
async fn redirect(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    // 数据库查询url
    let url = state.get_url(&id).await.map_err(|e| {
        warn!("#106:{}", e);
        StatusCode::NOT_FOUND
    })?;

    // 创建HTTP协议Header，并插入location头
    let mut header = HeaderMap::new();
    // url从String类型convert成Url类型，如果Url不合法抛出错误
    // TODO www.baidu.com返回response，浏览器无法解析时，当作相对路径发起重定向请求，造成错误
    // TODO 只有在url完整、解析成功时才会被当作绝对路径
    let url = url.parse().map_err(|e| {
        let e = ShortenError::UrlParse(format!("{} parse error:{}", url, e));
        warn!("#115:{}", e);
        StatusCode::NOT_FOUND
    })?;
    header.insert(LOCATION, url);

    // 返回状态码+header
    Ok((StatusCode::PERMANENT_REDIRECT, header))
}

impl AppState {
    async fn try_new(url: &str) -> Result<Self> {
        // 连接postgres
        let pool = PgPool::connect(url).await;
        let pool = match pool {
            Ok(p) => p,
            Err(e) => {
                return Err(ShortenError::Database(e.to_string()).into());
            }
        };
        // 执行创建urls sql
        sqlx::query(
            r#"
        create table if not exists urls(
            id char(6) primary key,
            url text unique not null
        )"#,
        )
        .execute(&pool)
        .await
        .map_err(ShortenError::SqlxQuery)?;

        Ok(Self { pool })
    }

    async fn add(&self, url: String) -> Result<String> {
        // 查询随机id是否重复
        #[allow(unused)]
        let mut id = String::default();
        loop {
            id = nanoid!(6);
            let ret: Vec<Urls> = sqlx::query_as("select url from urls where id=$1")
                .bind(&id)
                .fetch_all(&self.pool)
                .await?;
            if ret.is_empty() {
                break;
            }
        }
        // let id=nanoid!(6);
        // 要将返回的数据解构成结构体，不是serde的serialize；而是sql的FromRow trait
        // exclude.url使用新值更新
        let ret=sqlx::query_as::<_,Urls>(
            "insert into urls(id,url) values($1,$2) on conflict(url) do update set id=excluded.id returning id"
        )
        .bind(&id)
        .bind(&url)
        .fetch_one(&self.pool)
        .await;

        let ret = match ret {
            Ok(ret) => ret,
            Err(e) => {
                return Err(ShortenError::SqlxQuery(e).into());
            }
        };

        Ok(ret.id)
    }

    async fn get_url(&self, key: &str) -> Result<String> {
        let ret = sqlx::query_as::<_, Urls>("select url from urls where id=$1")
            .bind(key)
            .fetch_one(&self.pool)
            .await;
        let url = match ret {
            Ok(ret) => ret.url,
            Err(e) => {
                return Err(ShortenError::SqlxQuery(e).into());
            }
        };

        Ok(url)
    }
}
