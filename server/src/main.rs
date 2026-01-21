use actix_web::{web, App, HttpServer};
use sqlx::MySqlPool;
use dotenvy::dotenv;
use std::env;

mod auth;
mod routes;
mod security;
mod user_service;
use user_service::create_user;

use auth::ApiKeyAuth;
use routes::resorts::*;
use routes::slopes::*;
use routes::lifts::*;

use routes::auth::{signup, signin};

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
        .app_data(web::Data::new(pool.clone()))

        // Öffentliche Routen – garantiert ohne Auth
        .route("/signup", web::post().to(signup))
        .route("/signin", web::post().to(signin))

        // Geschützte Routen – explizit mit Auth
        .service(
            web::scope("")
                .wrap(ApiKeyAuth { pool: pool.clone() })

                // Resorts
                .route("/resorts", web::get().to(get_resorts))
                .route("/resorts/{id}", web::get().to(get_resort))
                .route("/resorts", web::post().to(create_resort))
                .route("/resorts/{id}", web::put().to(update_resort))
                .route("/resorts/{id}", web::delete().to(delete_resort))

                // Slopes
                .route("/slopes", web::get().to(get_slopes))
                .route("/slopes/{id}", web::get().to(get_slope))
                .route("/slopes", web::post().to(create_slope))
                .route("/slopes/{id}", web::put().to(update_slope))
                .route("/slopes/{id}", web::delete().to(delete_slope))

                // Lifts
                .route("/lifts", web::get().to(get_lifts))
                .route("/lifts/{id}", web::get().to(get_lift))
                .route("/lifts", web::post().to(create_lift))
                .route("/lifts/{id}", web::put().to(update_lift))
                .route("/lifts/{id}", web::delete().to(delete_lift))
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}