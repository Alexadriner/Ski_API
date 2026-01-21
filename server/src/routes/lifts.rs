use actix_web::{web, HttpResponse, Responder};
use sqlx::MySqlPool;
use serde::{Serialize, Deserialize};

#[derive(Serialize)]
pub struct Lift {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
}

#[derive(Deserialize)]
pub struct CreateLift {
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
}

#[derive(Deserialize)]
pub struct UpdateLift {
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
}

// GET /lifts
pub async fn get_lifts(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query_as!(
        Lift,
        r#"
        SELECT id, resort_id, name, lift_type
        FROM lifts
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(lifts) => HttpResponse::Ok().json(lifts),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

// GET /lifts/{id}
pub async fn get_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Lift,
        r#"
        SELECT id, resort_id, name, lift_type
        FROM lifts
        WHERE id = ?
        "#,
        *id
    )
    .fetch_one(db.get_ref())
    .await;

    match result {
        Ok(lift) => HttpResponse::Ok().json(lift),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

// POST /lifts (ADMIN)
pub async fn create_lift(
    db: web::Data<MySqlPool>,
    lift: web::Json<CreateLift>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO lifts (resort_id, name, lift_type)
        VALUES (?, ?, ?)
        "#,
        lift.resort_id,
        lift.name,
        lift.lift_type
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(_) => HttpResponse::Created().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

// PUT /lifts/{id} (ADMIN)
pub async fn update_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
    lift: web::Json<UpdateLift>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE lifts
        SET resort_id = ?, name = ?, lift_type = ?
        WHERE id = ?
        "#,
        lift.resort_id,
        lift.name,
        lift.lift_type,
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

// DELETE /lifts/{id} (ADMIN)
pub async fn delete_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query!(
        "DELETE FROM lifts WHERE id = ?",
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
