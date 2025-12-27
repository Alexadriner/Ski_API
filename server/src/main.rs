use actix_web::{web, App, HttpServer};
use sqlx::MySqlPool;
use dotenvy::dotenv;
use std::env;

mod auth;
mod routes;
mod security;
mod user_service;

use auth::ApiKeyAuth;
use routes::resorts::*;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL not set");

    let pool = MySqlPool::connect(&database_url)
        .await
        .expect("DB connection failed");

    println!("Server läuft auf Port 8080");

    HttpServer::new(move || {
        App::new()
            .wrap(ApiKeyAuth {
                pool: pool.clone(),
            })
            .app_data(web::Data::new(pool.clone()))
            .route("/resorts", web::get().to(get_resorts))
            .route("/resorts/{id}", web::get().to(get_resort))
            .route("/resorts", web::post().to(create_resort))
            .route("/resorts/{id}", web::put().to(update_resort))
            .route("/resorts/{id}", web::delete().to(delete_resort))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}