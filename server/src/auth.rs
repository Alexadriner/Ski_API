use actix_web::{
    body::{BoxBody, MessageBody},
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    http::Method,
    error::ErrorInternalServerError,
    Error, HttpResponse,
};
use futures_util::future::{ok, Ready, LocalBoxFuture};
use sqlx::MySqlPool;
use std::{
    rc::Rc,
    task::{Context, Poll},
};
use url::form_urlencoded;

use crate::security::hash::verify_secret;
use crate::security::subscription::get_limits;
use time::OffsetDateTime;

pub struct ApiKeyAuth {
    pub pool: MySqlPool,
}

impl<S, B> Transform<S, ServiceRequest> for ApiKeyAuth
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type InitError = ();
    type Transform = ApiKeyAuthMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(ApiKeyAuthMiddleware {
            service: Rc::new(service),
            pool: self.pool.clone(),
        })
    }
}

pub struct ApiKeyAuthMiddleware<S> {
    service: Rc<S>,
    pool: MySqlPool,
}

impl<S, B> Service<ServiceRequest> for ApiKeyAuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let srv = self.service.clone();
        let pool = self.pool.clone();
        let method = req.method().clone();

        Box::pin(async move {
            /* ---------------- API KEY LESEN ---------------- */

            let api_key = match form_urlencoded::parse(req.query_string().as_bytes())
                .find(|(k, _)| k == "api_key")
                .map(|(_, v)| v.to_string())
            {
                Some(k) => k,
                None => {
                    return Ok(req.into_response(
                        HttpResponse::Unauthorized()
                            .body("Missing api_key")
                            .map_into_boxed_body(),
                    ));
                }
            };

            /* ---------------- USER LADEN ---------------- */

            let users = sqlx::query!(
                r#"
                SELECT id, api_key, is_admin, subscription,
                       requests_minute, requests_month,
                       last_request_minute, last_request_month
                FROM users
                "#
            )
            .fetch_all(&pool)
            .await
            .map_err(|_| ErrorInternalServerError("Database error"))?;

            let mut user = None;

            for u in users {
                if verify_secret(&api_key, &u.api_key) {
                    user = Some(u);
                    break;
                }
            }

            let mut user = match user {
                Some(u) => u,
                None => {
                    return Ok(req.into_response(
                        HttpResponse::Unauthorized()
                            .body("Invalid api_key")
                            .map_into_boxed_body(),
                    ));
                }
            };

            /* ---------------- ADMIN CHECK ---------------- */

            if method != Method::GET && user.is_admin != 1 {
                return Ok(req.into_response(
                    HttpResponse::Forbidden()
                        .body("Admin privileges required")
                        .map_into_boxed_body(),
                ));
            }

            /* ---------------- RATE LIMIT ---------------- */

            let now = OffsetDateTime::now_utc();
            let today = now.date();

            let reset_minute = user
                .last_request_minute
                .map(|t| {
                    t.year() != now.year()
                        || t.month() != now.month()
                        || t.day() != now.day()
                        || t.hour() != now.hour()
                        || t.minute() != now.minute()
                })
                .unwrap_or(true);

            let reset_month = user
                .last_request_month
                .map(|d| d.year() != now.year() || d.month() != now.month())
                .unwrap_or(true);

            let mut req_min: u32 = if reset_minute { 0 } else { user.requests_minute } as u32;
            let mut req_mon: u32 = if reset_month { 0 } else { user.requests_month } as u32;

            let limits = get_limits(&user.subscription);

            if let Some(max ) = limits.per_minute {
                if req_min >= max {
                    return Ok(req.into_response(
                        HttpResponse::TooManyRequests()
                            .body("Minute rate limit exceeded")
                            .map_into_boxed_body(),
                    ));
                }
            }

            if let Some(max) = limits.per_month {
                if req_mon >= max {
                    return Ok(req.into_response(
                        HttpResponse::TooManyRequests()
                            .body("Monthly rate limit exceeded")
                            .map_into_boxed_body(),
                    ));
                }
            }

            req_min += 1;
            req_mon += 1;

            sqlx::query!(
                r#"
                UPDATE users
                SET
                    requests_minute = ?,
                    requests_month = ?,
                    last_request_minute = ?,
                    last_request_month = ?
                WHERE id = ?
                "#,
                req_min,
                req_mon,
                now,
                today,
                user.id
            )
            .execute(&pool)
            .await
            .map_err(|_| ErrorInternalServerError("DB update failed"))?;

            /* ---------------- REQUEST WEITERLEITEN ---------------- */

            let res = srv.call(req).await?;
            Ok(res.map_into_boxed_body())
        })
    }
}