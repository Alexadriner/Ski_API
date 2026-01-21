use actix_web::{web, HttpResponse, Responder};
use sqlx::MySqlPool;
use serde::{Serialize, Deserialize};

#[derive(Serialize)]
pub struct Slope {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
}

#[derive(Deserialize)]
pub struct CreateSlope {
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
}

#[derive(Deserialize)]
pub struct UpdateSlope {
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
}

// GET /slopes
pub async fn get_slopes(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query_as!(
        Slope,
        r#"
        SELECT id, resort_id, name, difficulty
        FROM slopes
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(slopes) => HttpResponse::Ok().json(slopes),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

// GET /slopes/{id}
pub async fn get_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Slope,
        r#"
        SELECT id, resort_id, name, difficulty
        FROM slopes
        WHERE id = ?
        "#,
        *id
    )
    .fetch_one(db.get_ref())
    .await;

    match result {
        Ok(slope) => HttpResponse::Ok().json(slope),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

// POST /slopes (ADMIN)
pub async fn create_slope(
    db: web::Data<MySqlPool>,
    slope: web::Json<CreateSlope>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO slopes (resort_id, name, difficulty)
        VALUES (?, ?, ?)
        "#,
        slope.resort_id,
        slope.name,
        slope.difficulty
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(_) => HttpResponse::Created().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

// PUT /slopes/{id} (ADMIN)
pub async fn update_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
    slope: web::Json<UpdateSlope>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE slopes
        SET resort_id = ?, name = ?, difficulty = ?
        WHERE id = ?
        "#,
        slope.resort_id,
        slope.name,
        slope.difficulty,
        *id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) if res.rows_affected() == 0 => HttpResponse::NotFound().finish(),
        Ok(_) => HttpResponse::Ok().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

// DELETE /slopes/{id} (ADMIN)
pub async fn delete_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query!(
        "DELETE FROM slopes WHERE id = ?",
        *id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) if res.rows_affected() == 0 => HttpResponse::NotFound().finish(),
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}