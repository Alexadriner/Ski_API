use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;
use url::form_urlencoded;

use crate::user_service::create_user;
use crate::security::api_key::generate_api_key;
use crate::security::hash::hash_secret;
use crate::security::hash::verify_secret;

#[derive(Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct SigninRequest {
    pub email: Option<String>,
    pub username: Option<String>,
    pub password: String,
}

#[derive(Serialize)]
pub struct AuthUser {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub is_admin: bool,
    pub subscription: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub api_key: String,
    pub user: AuthUser,
}

pub async fn signup(
    pool: web::Data<MySqlPool>,
    data: web::Json<SignupRequest>,
) -> impl Responder {
    match create_user(
        pool.get_ref(),
        &data.username,
        &data.email,
        &data.password,
    )
    .await
    {
        Ok(api_key) => {
            let user = sqlx::query!(
                r#"
                SELECT id, name, email, is_admin, subscription
                FROM users
                WHERE email = ?
                "#,
                data.email
            )
            .fetch_optional(pool.get_ref())
            .await;

            match user {
                Ok(Some(u)) => HttpResponse::Created().json(AuthResponse {
                    api_key,
                    user: AuthUser {
                        id: u.id,
                        name: u.name,
                        email: u.email,
                        is_admin: u.is_admin == 1,
                        subscription: u.subscription,
                    },
                }),
                _ => HttpResponse::InternalServerError().body("Could not load user data"),
            }
        }
        Err(e) => {
            eprintln!("Signup failed: {}", e);
            HttpResponse::BadRequest().body("User already exists")
        }
    }
}

pub async fn signin(
    pool: web::Data<MySqlPool>,
    data: web::Json<SigninRequest>,
) -> impl Responder {
    let email = data.email.as_deref().unwrap_or("").trim();
    let username = data.username.as_deref().unwrap_or("").trim();
    let identifier = if !email.is_empty() { email } else { username };

    if identifier.is_empty() {
        return HttpResponse::BadRequest().body("Missing username or email");
    }

    let user = sqlx::query!(
        r#"
        SELECT id, name, email, password_hash, is_admin, subscription
        FROM users
        WHERE email = ? OR name = ?
        LIMIT 1
        "#,
        identifier,
        identifier
    )
    .fetch_optional(pool.get_ref())
    .await;

    let user = match user {
        Ok(Some(u)) => u,
        _ => return HttpResponse::Unauthorized().body("Invalid credentials"),
    };

    if !verify_secret(&data.password, &user.password_hash) {
        return HttpResponse::Unauthorized().body("Invalid credentials");
    }

    let api_key_plain = generate_api_key();
    let api_key_hash = hash_secret(&api_key_plain);

    let updated = sqlx::query!(
        r#"
        UPDATE users
        SET api_key = ?
        WHERE id = ?
        "#,
        api_key_hash,
        user.id
    )
    .execute(pool.get_ref())
    .await;

    match updated {
        Ok(_) => HttpResponse::Ok().json(AuthResponse {
            api_key: api_key_plain,
            user: AuthUser {
                id: user.id,
                name: user.name,
                email: user.email,
                is_admin: user.is_admin == 1,
                subscription: user.subscription,
            },
        }),
        Err(_) => HttpResponse::InternalServerError().body("Could not update api key"),
    }
}

pub async fn me(
    pool: web::Data<MySqlPool>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let api_key = match form_urlencoded::parse(req.query_string().as_bytes())
        .find(|(k, _)| k == "api_key")
        .map(|(_, v)| v.to_string())
    {
        Some(k) => k,
        None => return HttpResponse::Unauthorized().body("Missing api_key"),
    };

    let users = sqlx::query!(
        r#"
        SELECT id, name, email, api_key, is_admin, subscription
        FROM users
        "#
    )
    .fetch_all(pool.get_ref())
    .await;

    let users = match users {
        Ok(u) => u,
        Err(_) => return HttpResponse::InternalServerError().body("Database error"),
    };

    for u in users {
        if verify_secret(&api_key, &u.api_key) {
            return HttpResponse::Ok().json(AuthUser {
                id: u.id,
                name: u.name,
                email: u.email,
                is_admin: u.is_admin == 1,
                subscription: u.subscription,
            });
        }
    }

    HttpResponse::Unauthorized().body("Invalid api_key")
}
