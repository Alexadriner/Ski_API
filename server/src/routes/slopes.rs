use actix_web::{web, HttpResponse, Responder};
use sqlx::MySqlPool;
use serde::{Serialize, Deserialize};

#[derive(Serialize)]
pub struct Slope {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

#[derive(Deserialize)]
pub struct CreateSlope {
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

#[derive(Deserialize)]
pub struct UpdateSlope {
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

//
// GET /slopes
//
pub async fn get_slopes(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query_as!(
        Slope,
        r#"
        SELECT id, resort_id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start,
               CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end,
               CAST(lon_end AS DOUBLE) AS lon_end
        FROM slopes
        ORDER BY resort_id, name
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(slopes) => HttpResponse::Ok().json(slopes),
        Err(err) => {
            eprintln!("GET /slopes error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

//
// GET /slopes/{id}
//
pub async fn get_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Slope,
        r#"
        SELECT id, resort_id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start,
               CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end,
               CAST(lon_end AS DOUBLE) AS lon_end
        FROM slopes
        WHERE id = ?
        "#,
        *id
    )
    .fetch_optional(db.get_ref())
    .await;

    match result {
        Ok(Some(slope)) => HttpResponse::Ok().json(slope),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => {
            eprintln!("GET /slopes/{{id}} error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

//
// GET /slopes/by_resort/{resort_id}
//
pub async fn get_slopes_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query_as!(
        Slope,
        r#"
        SELECT id, resort_id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start,
               CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end,
               CAST(lon_end AS DOUBLE) AS lon_end
        FROM slopes
        WHERE resort_id = ?
        ORDER BY name
        "#,
        resort_id.into_inner()
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(slopes) => HttpResponse::Ok().json(slopes),
        Err(err) => {
            eprintln!("GET /slopes/by_resort error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

//
// POST /slopes
//
pub async fn create_slope(
    db: web::Data<MySqlPool>,
    slope: web::Json<CreateSlope>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO slopes
        (resort_id, name, difficulty, lat_start, lon_start, lat_end, lon_end)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
        slope.resort_id,
        slope.name,
        slope.difficulty,
        slope.lat_start,
        slope.lon_start,
        slope.lat_end,
        slope.lon_end
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) => HttpResponse::Created().json(res.last_insert_id()),
        Err(err) => {
            eprintln!("POST /slopes error: {:?}", err);
            HttpResponse::BadRequest().finish()
        }
    }
}

//
// PUT /slopes/{id}
//
pub async fn update_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
    slope: web::Json<UpdateSlope>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE slopes
        SET resort_id = ?, name = ?, difficulty = ?,
            lat_start = ?, lon_start = ?, lat_end = ?, lon_end = ?
        WHERE id = ?
        "#,
        slope.resort_id,
        slope.name,
        slope.difficulty,
        slope.lat_start,
        slope.lon_start,
        slope.lat_end,
        slope.lon_end,
        *id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) if res.rows_affected() == 0 => HttpResponse::NotFound().finish(),
        Ok(_) => HttpResponse::Ok().finish(),
        Err(err) => {
            eprintln!("PUT /slopes/{{id}} error: {:?}", err);
            HttpResponse::BadRequest().finish()
        }
    }
}

//
// DELETE /slopes/{id}
//
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
        Err(err) => {
            eprintln!("DELETE /slopes/{{id}} error: {:?}", err);
            HttpResponse::BadRequest().finish()
        }
    }
}

//
// DELETE /slopes/by_resort/{resort_id}
//
pub async fn delete_slopes_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!(
        "DELETE FROM slopes WHERE resort_id = ?",
        resort_id.into_inner()
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) => HttpResponse::Ok().json(res.rows_affected()),
        Err(err) => {
            eprintln!("DELETE /slopes/by_resort error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}
