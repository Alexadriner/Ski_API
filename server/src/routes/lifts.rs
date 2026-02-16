use actix_web::{web, HttpResponse, Responder};
use sqlx::MySqlPool;
use serde::{Serialize, Deserialize};

#[derive(Serialize)]
pub struct Lift {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

#[derive(Deserialize)]
pub struct CreateLift {
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

#[derive(Deserialize)]
pub struct UpdateLift {
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

//
// GET /lifts
//
pub async fn get_lifts(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query_as!(
        Lift,
        r#"
        SELECT id, resort_id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start,
               CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end,
               CAST(lon_end AS DOUBLE) AS lon_end
        FROM lifts
        ORDER BY resort_id, name
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(lifts) => HttpResponse::Ok().json(lifts),
        Err(err) => {
            eprintln!("GET /lifts error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

//
// GET /lifts/{id}
//
pub async fn get_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Lift,
        r#"
        SELECT id, resort_id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start,
               CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end,
               CAST(lon_end AS DOUBLE) AS lon_end
        FROM lifts
        WHERE id = ?
        "#,
        *id
    )
    .fetch_optional(db.get_ref())
    .await;

    match result {
        Ok(Some(lift)) => HttpResponse::Ok().json(lift),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => {
            eprintln!("GET /lifts/{{id}} error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

//
// GET /lifts/by_resort/{resort_id}
//
pub async fn get_lifts_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Lift,
        r#"
        SELECT id, resort_id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start,
               CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end,
               CAST(lon_end AS DOUBLE) AS lon_end
        FROM lifts
        WHERE resort_id = ?
        ORDER BY name
        "#,
        resort_id.into_inner()
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(lifts) => HttpResponse::Ok().json(lifts),
        Err(err) => {
            eprintln!("GET /lifts/by_resort error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

//
// POST /lifts
//
pub async fn create_lift(
    db: web::Data<MySqlPool>,
    lift: web::Json<CreateLift>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO lifts
        (resort_id, name, lift_type, lat_start, lon_start, lat_end, lon_end)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
        lift.resort_id,
        lift.name,
        lift.lift_type,
        lift.lat_start,
        lift.lon_start,
        lift.lat_end,
        lift.lon_end
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) => HttpResponse::Created().json(res.last_insert_id()),
        Err(err) => {
            eprintln!("POST /lifts error: {:?}", err);
            HttpResponse::BadRequest().finish()
        }
    }
}

//
// PUT /lifts/{id}
//
pub async fn update_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
    lift: web::Json<UpdateLift>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE lifts
        SET resort_id = ?, name = ?, lift_type = ?,
            lat_start = ?, lon_start = ?, lat_end = ?, lon_end = ?
        WHERE id = ?
        "#,
        lift.resort_id,
        lift.name,
        lift.lift_type,
        lift.lat_start,
        lift.lon_start,
        lift.lat_end,
        lift.lon_end,
        *id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) if res.rows_affected() == 0 => HttpResponse::NotFound().finish(),
        Ok(_) => HttpResponse::Ok().finish(),
        Err(err) => {
            eprintln!("PUT /lifts/{{id}} error: {:?}", err);
            HttpResponse::BadRequest().finish()
        }
    }
}

//
// DELETE /lifts/{id}
//
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
        Err(err) => {
            eprintln!("DELETE /lifts/{{id}} error: {:?}", err);
            HttpResponse::BadRequest().finish()
        }
    }
}

//
// DELETE /lifts/by_resort/{resort_id}
// WICHTIG für OSM-Rebuild
//
pub async fn delete_lifts_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!(
        "DELETE FROM lifts WHERE resort_id = ?",
        resort_id.into_inner()
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) => HttpResponse::Ok().json(res.rows_affected()),
        Err(err) => {
            eprintln!("DELETE /lifts/by_resort error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}
