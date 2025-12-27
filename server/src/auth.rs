use actix_web::{
    body::{BoxBody, MessageBody},
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    http::Method,
    Error, HttpResponse,
    error::ErrorInternalServerError
};
use futures_util::future::{ok, Ready, LocalBoxFuture};
use sqlx::MySqlPool;
use std::{
    rc::Rc,
    task::{Context, Poll},
};
use url::form_urlencoded;

use crate::security::hash::verify_secret;

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
            // --- api_key aus URL ---
            let query = req.query_string();
            let provided_key = form_urlencoded::parse(query.as_bytes())
                .find(|(k, _)| k == "api_key")
                .map(|(_, v)| v.to_string());

            let api_key = match provided_key {
                Some(k) => k,
                None => {
                    let res = HttpResponse::Unauthorized()
                        .body("Missing api_key");
                    return Ok(req.into_response(res.map_into_boxed_body()));
                }
            };

            // --- alle User laden ---
            let users = sqlx::query!(
                "SELECT api_key, is_admin FROM users"
            )
            .fetch_all(&pool)
            .await
            .map_err(|_| ErrorInternalServerError("Database error"))?;

            // --- Key verifizieren ---
            let mut is_admin = false;
            let mut valid = false;

            for user in users {
                let stored_hash = user.api_key.as_str();

                if verify_secret(&api_key, stored_hash) {
                    valid = true;
                    is_admin = user.is_admin == 1;
                    break;
                }
            }

            if !valid {
                let res = HttpResponse::Unauthorized()
                    .body("Invalid api_key");
                return Ok(req.into_response(res.map_into_boxed_body()));
            }

            // --- Rechte prüfen ---
            let is_get = method == Method::GET;

            if !is_get && !is_admin {
                let res = HttpResponse::Forbidden()
                    .body("Admin privileges required");
                return Ok(req.into_response(res.map_into_boxed_body()));
            }

            // --- Request weiterreichen ---
            let res = srv.call(req).await?;
            Ok(res.map_into_boxed_body())
        })
    }
}