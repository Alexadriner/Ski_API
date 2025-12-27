use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

use crate::user_service::create_user;
use crate::security::hash::verify_secret;

#[derive(Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct SigninRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct SignupResponse {
    pub api_key: String,
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
        Ok(api_key) => HttpResponse::Created().json(SignupResponse {
            api_key,
        }),
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
    let user = sqlx::query!(
        r#"
        SELECT password_hash
        FROM users
        WHERE email = ?
        "#,
        data.email
    )
    .fetch_optional(pool.get_ref())
    .await;

    let user = match user {
        Ok(Some(u)) => u,
        _ => return HttpResponse::Unauthorized().body("Invalid credentials"),
    };

    if verify_secret(&data.password, &user.password_hash) {
        HttpResponse::Ok().body("Login successful")
    } else {
        HttpResponse::Unauthorized().body("Invalid credentials")
    }
}