use actix_web::{web, HttpResponse, Responder};
use sqlx::MySqlPool;
use serde::{Serialize, Deserialize};

/* ---------- MODEL ---------- */

#[derive(Serialize, Deserialize)]
pub struct Resort {
    pub id: String,
    pub name: String,
    pub country: String,
    pub region: Option<String>,
    pub continent: Option<String>,

    pub latitude: Option<f64>,
    pub longitude: Option<f64>,

    pub village_altitude_m: Option<i32>,
    pub min_altitude_m: Option<i32>,
    pub max_altitude_m: Option<i32>,

    pub ski_area_name: Option<String>,
    pub ski_area_type: Option<String>,
}

/* ---------- HANDLER ---------- */

// GET /resorts
pub async fn get_resorts(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query_as!(
        Resort,
        r#"
        SELECT id, name, country, region, continent,
               latitude, longitude, village_altitude_m,
               min_altitude_m, max_altitude_m, ski_area_name, ski_area_type
        FROM resorts
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(resorts) => HttpResponse::Ok().json(resorts),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

// GET /resorts/{id}
pub async fn get_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Resort,
        r#"
        SELECT id, name, country, region, continent,
               latitude, longitude, village_altitude_m,
               min_altitude_m, max_altitude_m, ski_area_name, ski_area_type
        FROM resorts WHERE id = ?
        "#,
        *id
    )
    .fetch_one(db.get_ref())
    .await;

    match result {
        Ok(resort) => HttpResponse::Ok().json(resort),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

// POST /resorts
pub async fn create_resort(
    db: web::Data<MySqlPool>,
    resort: web::Json<Resort>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO resorts
        (id, name, country, region, continent,
         latitude, longitude, village_altitude_m,
         min_altitude_m, max_altitude_m, ski_area_name, ski_area_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        resort.id,
        resort.name,
        resort.country,
        resort.region,
        resort.continent,
        resort.latitude,
        resort.longitude,
        resort.village_altitude_m,
        resort.min_altitude_m,
        resort.max_altitude_m,
        resort.ski_area_name,
        resort.ski_area_type
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(_) => HttpResponse::Created().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

// PUT /resorts/{id}
pub async fn update_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
    resort: web::Json<Resort>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE resorts SET
            name = ?, country = ?, region = ?, continent = ?,
            latitude = ?, longitude = ?, village_altitude_m = ?,
            min_altitude_m = ?, max_altitude_m = ?, ski_area_name = ?, ski_area_type = ?
        WHERE id = ?
        "#,
        resort.name,
        resort.country,
        resort.region,
        resort.continent,
        resort.latitude,
        resort.longitude,
        resort.village_altitude_m,
        resort.min_altitude_m,
        resort.max_altitude_m,
        resort.ski_area_name,
        resort.ski_area_type,
        *id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

// DELETE /resorts/{id}
pub async fn delete_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!(
        "DELETE FROM resorts WHERE id = ?",
        *id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}