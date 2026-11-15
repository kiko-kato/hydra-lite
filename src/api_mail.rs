use crate::AppState;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

pub(crate) async fn handle_mail_proxy(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
) -> Response {
    // 1. Enforce strict Admin authentication
    if crate::api_keys::extract_caller(&headers, &state).await.is_none() {
        return (StatusCode::UNAUTHORIZED, "Admin authentication required").into_response();
    }

    // 2. Build target URI on localhost:8105
    let uri = req.uri();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/api/mail");
    let subpath = path_and_query.strip_prefix("/api/mail").unwrap_or(path_and_query);
    let target_url = format!("http://127.0.0.1:8105/api{}", subpath);

    let method = req.method().clone();
    let (parts, body) = req.into_parts();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();

    let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, "Payload too large").into_response(),
    };

    let mut req_builder = client.request(method, &target_url);
    if let Some(ct) = parts.headers.get("content-type") {
        req_builder = req_builder.header("content-type", ct);
    }

    match req_builder.body(bytes).send().await {
        Ok(upstream_resp) => {
            let status = StatusCode::from_u16(upstream_resp.status().as_u16()).unwrap_or(StatusCode::OK);
            let ct = upstream_resp.headers().get("content-type").cloned();
            let body_bytes = upstream_resp.bytes().await.unwrap_or_default();

            let mut resp = Response::builder().status(status);
            if let Some(ct) = ct {
                resp = resp.header("content-type", ct);
            }
            resp.body(Body::from(body_bytes)).unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "Response build error").into_response()
            })
        }
        Err(e) => {
            log::error!("[MAIL:PROXY:ERR] Failed to reach local mail daemon: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                format!("Mail daemon offline or unreachable: {}", e),
            )
                .into_response()
        }
    }
}
