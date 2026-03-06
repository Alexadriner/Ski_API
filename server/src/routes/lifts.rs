use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

#[derive(Serialize)]
pub struct Lift {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub display: LiftDisplay,
    pub geometry: LiftGeometry,
    pub specs: LiftSpecs,
    pub source: LiftSource,
    pub status: LiftStatus,
}

#[derive(Serialize)]
pub struct LiftDisplay {
    pub normalized_name: Option<String>,
    pub lift_type: String,
}

#[derive(Serialize)]
pub struct LiftGeometry {
    pub start: CoordinatePoint,
    pub end: CoordinatePoint,
}

#[derive(Serialize)]
pub struct CoordinatePoint {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Serialize)]
pub struct LiftSpecs {
    pub capacity_per_hour: Option<i32>,
    pub seats: Option<i8>,
    pub bubble: bool,
    pub heated_seats: bool,
    pub year_built: Option<i16>,
    pub altitude_start_m: Option<i32>,
    pub altitude_end_m: Option<i32>,
}

#[derive(Serialize)]
pub struct LiftSource {
    pub system: String,
    pub entity_id: Option<String>,
    pub source_url: Option<String>,
}

#[derive(Serialize)]
pub struct LiftStatus {
    pub operational_status: String,
    pub note: Option<String>,
    pub planned_open_time: Option<String>,
    pub planned_close_time: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateLift {
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
    pub capacity_per_hour: Option<i32>,
    pub seats: Option<i8>,
    pub bubble: Option<bool>,
    pub heated_seats: Option<bool>,
    pub year_built: Option<i16>,
    pub altitude_start_m: Option<i32>,
    pub altitude_end_m: Option<i32>,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
    pub source_system: Option<String>,
    pub source_entity_id: Option<String>,
    pub name_normalized: Option<String>,
    pub operational_status: Option<String>,
    pub operational_note: Option<String>,
    pub planned_open_time: Option<String>,
    pub planned_close_time: Option<String>,
    pub status_updated_at: Option<String>,
    pub status_source_url: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateLift {
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
    pub capacity_per_hour: Option<i32>,
    pub seats: Option<i8>,
    pub bubble: Option<bool>,
    pub heated_seats: Option<bool>,
    pub year_built: Option<i16>,
    pub altitude_start_m: Option<i32>,
    pub altitude_end_m: Option<i32>,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
    pub source_system: Option<String>,
    pub source_entity_id: Option<String>,
    pub name_normalized: Option<String>,
    pub operational_status: Option<String>,
    pub operational_note: Option<String>,
    pub planned_open_time: Option<String>,
    pub planned_close_time: Option<String>,
    pub status_updated_at: Option<String>,
    pub status_source_url: Option<String>,
}

pub async fn get_lifts(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, lift_type, name_normalized,
               capacity_per_hour, seats, bubble, heated_seats, year_built, altitude_start_m, altitude_end_m,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               source_system, source_entity_id, operational_status, operational_note, status_source_url,
               DATE_FORMAT(planned_open_time, '%H:%i:%s') AS planned_open_time,
               DATE_FORMAT(planned_close_time, '%H:%i:%s') AS planned_close_time,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM lifts
        ORDER BY resort_id, name
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|row| Lift {
                    id: row.id,
                    resort_id: row.resort_id,
                    name: row.name,
                    display: LiftDisplay {
                        normalized_name: row.name_normalized,
                        lift_type: row.lift_type,
                    },
                    geometry: LiftGeometry {
                        start: CoordinatePoint {
                            latitude: row.lat_start,
                            longitude: row.lon_start,
                        },
                        end: CoordinatePoint {
                            latitude: row.lat_end,
                            longitude: row.lon_end,
                        },
                    },
                    specs: LiftSpecs {
                        capacity_per_hour: row.capacity_per_hour,
                        seats: row.seats,
                        bubble: row.bubble.unwrap_or(0) != 0,
                        heated_seats: row.heated_seats.unwrap_or(0) != 0,
                        year_built: row.year_built,
                        altitude_start_m: row.altitude_start_m,
                        altitude_end_m: row.altitude_end_m,
                    },
                    source: LiftSource {
                        system: row.source_system,
                        entity_id: row.source_entity_id,
                        source_url: row.status_source_url,
                    },
                    status: LiftStatus {
                        operational_status: row.operational_status,
                        note: row.operational_note,
                        planned_open_time: row.planned_open_time,
                        planned_close_time: row.planned_close_time,
                        updated_at: row.status_updated_at,
                    },
                })
                .collect::<Vec<Lift>>(),
        ),
        Err(err) => {
            eprintln!("GET /lifts error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_lift(db: web::Data<MySqlPool>, id: web::Path<i64>) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, lift_type, name_normalized,
               capacity_per_hour, seats, bubble, heated_seats, year_built, altitude_start_m, altitude_end_m,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               source_system, source_entity_id, operational_status, operational_note, status_source_url,
               DATE_FORMAT(planned_open_time, '%H:%i:%s') AS planned_open_time,
               DATE_FORMAT(planned_close_time, '%H:%i:%s') AS planned_close_time,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM lifts
        WHERE id = ?
        "#,
        *id
    )
    .fetch_optional(db.get_ref())
    .await;

    match result {
        Ok(Some(row)) => HttpResponse::Ok().json(Lift {
            id: row.id,
            resort_id: row.resort_id,
            name: row.name,
            display: LiftDisplay {
                normalized_name: row.name_normalized,
                lift_type: row.lift_type,
            },
            geometry: LiftGeometry {
                start: CoordinatePoint {
                    latitude: row.lat_start,
                    longitude: row.lon_start,
                },
                end: CoordinatePoint {
                    latitude: row.lat_end,
                    longitude: row.lon_end,
                },
            },
            specs: LiftSpecs {
                capacity_per_hour: row.capacity_per_hour,
                seats: row.seats,
                bubble: row.bubble.unwrap_or(0) != 0,
                heated_seats: row.heated_seats.unwrap_or(0) != 0,
                year_built: row.year_built,
                altitude_start_m: row.altitude_start_m,
                altitude_end_m: row.altitude_end_m,
            },
            source: LiftSource {
                system: row.source_system,
                entity_id: row.source_entity_id,
                source_url: row.status_source_url,
            },
            status: LiftStatus {
                operational_status: row.operational_status,
                note: row.operational_note,
                planned_open_time: row.planned_open_time,
                planned_close_time: row.planned_close_time,
                updated_at: row.status_updated_at,
            },
        }),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => {
            eprintln!("GET /lifts/{{id}} error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_lifts_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, lift_type, name_normalized,
               capacity_per_hour, seats, bubble, heated_seats, year_built, altitude_start_m, altitude_end_m,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               source_system, source_entity_id, operational_status, operational_note, status_source_url,
               DATE_FORMAT(planned_open_time, '%H:%i:%s') AS planned_open_time,
               DATE_FORMAT(planned_close_time, '%H:%i:%s') AS planned_close_time,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM lifts
        WHERE resort_id = ?
        ORDER BY name
        "#,
        resort_id.into_inner()
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|row| Lift {
                    id: row.id,
                    resort_id: row.resort_id,
                    name: row.name,
                    display: LiftDisplay {
                        normalized_name: row.name_normalized,
                        lift_type: row.lift_type,
                    },
                    geometry: LiftGeometry {
                        start: CoordinatePoint {
                            latitude: row.lat_start,
                            longitude: row.lon_start,
                        },
                        end: CoordinatePoint {
                            latitude: row.lat_end,
                            longitude: row.lon_end,
                        },
                    },
                    specs: LiftSpecs {
                        capacity_per_hour: row.capacity_per_hour,
                        seats: row.seats,
                        bubble: row.bubble.unwrap_or(0) != 0,
                        heated_seats: row.heated_seats.unwrap_or(0) != 0,
                        year_built: row.year_built,
                        altitude_start_m: row.altitude_start_m,
                        altitude_end_m: row.altitude_end_m,
                    },
                    source: LiftSource {
                        system: row.source_system,
                        entity_id: row.source_entity_id,
                        source_url: row.status_source_url,
                    },
                    status: LiftStatus {
                        operational_status: row.operational_status,
                        note: row.operational_note,
                        planned_open_time: row.planned_open_time,
                        planned_close_time: row.planned_close_time,
                        updated_at: row.status_updated_at,
                    },
                })
                .collect::<Vec<Lift>>(),
        ),
        Err(err) => {
            eprintln!("GET /lifts/by_resort error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn create_lift(
    db: web::Data<MySqlPool>,
    lift: web::Json<CreateLift>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO lifts
        (resort_id, name, lift_type,
         capacity_per_hour, seats, bubble, heated_seats, year_built, altitude_start_m, altitude_end_m,
         lat_start, lon_start, lat_end, lon_end,
         source_system, source_entity_id, name_normalized,
         operational_status, operational_note, planned_open_time, planned_close_time, status_updated_at, status_source_url)
        VALUES (?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?)
        "#,
        lift.resort_id,
        lift.name,
        lift.lift_type,
        lift.capacity_per_hour,
        lift.seats,
        lift.bubble.unwrap_or(false),
        lift.heated_seats.unwrap_or(false),
        lift.year_built,
        lift.altitude_start_m,
        lift.altitude_end_m,
        lift.lat_start,
        lift.lon_start,
        lift.lat_end,
        lift.lon_end,
        lift.source_system.as_deref().unwrap_or("osm"),
        lift.source_entity_id,
        lift.name_normalized,
        lift.operational_status.as_deref().unwrap_or("unknown"),
        lift.operational_note,
        lift.planned_open_time,
        lift.planned_close_time,
        lift.status_updated_at,
        lift.status_source_url
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

pub async fn update_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
    lift: web::Json<UpdateLift>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE lifts
        SET resort_id = ?, name = ?, lift_type = ?,
            capacity_per_hour = ?, seats = ?, bubble = ?, heated_seats = ?, year_built = ?, altitude_start_m = ?, altitude_end_m = ?,
            lat_start = ?, lon_start = ?, lat_end = ?, lon_end = ?,
            source_system = ?, source_entity_id = ?, name_normalized = ?,
            operational_status = ?, operational_note = ?, planned_open_time = ?, planned_close_time = ?, status_updated_at = ?, status_source_url = ?
        WHERE id = ?
        "#,
        lift.resort_id,
        lift.name,
        lift.lift_type,
        lift.capacity_per_hour,
        lift.seats,
        lift.bubble.unwrap_or(false),
        lift.heated_seats.unwrap_or(false),
        lift.year_built,
        lift.altitude_start_m,
        lift.altitude_end_m,
        lift.lat_start,
        lift.lon_start,
        lift.lat_end,
        lift.lon_end,
        lift.source_system.as_deref().unwrap_or("osm"),
        lift.source_entity_id,
        lift.name_normalized,
        lift.operational_status.as_deref().unwrap_or("unknown"),
        lift.operational_note,
        lift.planned_open_time,
        lift.planned_close_time,
        lift.status_updated_at,
        lift.status_source_url,
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

pub async fn delete_lift(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query!("DELETE FROM lifts WHERE id = ?", *id)
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

pub async fn delete_lifts_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!("DELETE FROM lifts WHERE resort_id = ?", resort_id.into_inner())
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
