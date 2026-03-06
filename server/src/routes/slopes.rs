use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

#[derive(Serialize)]
pub struct Slope {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub display: SlopeDisplay,
    pub geometry: SlopeGeometry,
    pub specs: SlopeSpecs,
    pub source: SlopeSource,
    pub status: SlopeStatus,
}

#[derive(Serialize)]
pub struct SlopeDisplay {
    pub normalized_name: Option<String>,
    pub difficulty: String,
}

#[derive(Serialize)]
pub struct SlopeGeometry {
    pub start: CoordinatePoint,
    pub end: CoordinatePoint,
}

#[derive(Serialize)]
pub struct CoordinatePoint {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Serialize)]
pub struct SlopeSpecs {
    pub length_m: Option<i32>,
    pub vertical_drop_m: Option<i32>,
    pub average_gradient: Option<f64>,
    pub max_gradient: Option<f64>,
    pub snowmaking: bool,
    pub night_skiing: bool,
    pub family_friendly: bool,
    pub race_slope: bool,
}

#[derive(Serialize)]
pub struct SlopeSource {
    pub system: String,
    pub entity_id: Option<String>,
    pub source_url: Option<String>,
}

#[derive(Serialize)]
pub struct SlopeStatus {
    pub operational_status: String,
    pub grooming_status: String,
    pub note: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateSlope {
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
    pub length_m: Option<i32>,
    pub vertical_drop_m: Option<i32>,
    pub average_gradient: Option<f64>,
    pub max_gradient: Option<f64>,
    pub snowmaking: Option<bool>,
    pub night_skiing: Option<bool>,
    pub family_friendly: Option<bool>,
    pub race_slope: Option<bool>,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
    pub source_system: Option<String>,
    pub source_entity_id: Option<String>,
    pub name_normalized: Option<String>,
    pub operational_status: Option<String>,
    pub grooming_status: Option<String>,
    pub operational_note: Option<String>,
    pub status_updated_at: Option<String>,
    pub status_source_url: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateSlope {
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
    pub length_m: Option<i32>,
    pub vertical_drop_m: Option<i32>,
    pub average_gradient: Option<f64>,
    pub max_gradient: Option<f64>,
    pub snowmaking: Option<bool>,
    pub night_skiing: Option<bool>,
    pub family_friendly: Option<bool>,
    pub race_slope: Option<bool>,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
    pub source_system: Option<String>,
    pub source_entity_id: Option<String>,
    pub name_normalized: Option<String>,
    pub operational_status: Option<String>,
    pub grooming_status: Option<String>,
    pub operational_note: Option<String>,
    pub status_updated_at: Option<String>,
    pub status_source_url: Option<String>,
}

pub async fn get_slopes(db: web::Data<MySqlPool>) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, difficulty, name_normalized,
               length_m, vertical_drop_m,
               CAST(average_gradient AS DOUBLE) AS average_gradient,
               CAST(max_gradient AS DOUBLE) AS max_gradient,
               snowmaking, night_skiing, family_friendly, race_slope,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               source_system, source_entity_id, operational_status, grooming_status, operational_note, status_source_url,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM slopes
        ORDER BY resort_id, name
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|row| Slope {
                    id: row.id,
                    resort_id: row.resort_id,
                    name: row.name,
                    display: SlopeDisplay {
                        normalized_name: row.name_normalized,
                        difficulty: row.difficulty,
                    },
                    geometry: SlopeGeometry {
                        start: CoordinatePoint {
                            latitude: row.lat_start,
                            longitude: row.lon_start,
                        },
                        end: CoordinatePoint {
                            latitude: row.lat_end,
                            longitude: row.lon_end,
                        },
                    },
                    specs: SlopeSpecs {
                        length_m: row.length_m,
                        vertical_drop_m: row.vertical_drop_m,
                        average_gradient: row.average_gradient,
                        max_gradient: row.max_gradient,
                        snowmaking: row.snowmaking.unwrap_or(0) != 0,
                        night_skiing: row.night_skiing.unwrap_or(0) != 0,
                        family_friendly: row.family_friendly.unwrap_or(0) != 0,
                        race_slope: row.race_slope.unwrap_or(0) != 0,
                    },
                    source: SlopeSource {
                        system: row.source_system,
                        entity_id: row.source_entity_id,
                        source_url: row.status_source_url,
                    },
                    status: SlopeStatus {
                        operational_status: row.operational_status,
                        grooming_status: row.grooming_status,
                        note: row.operational_note,
                        updated_at: row.status_updated_at,
                    },
                })
                .collect::<Vec<Slope>>(),
        ),
        Err(err) => {
            eprintln!("GET /slopes error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_slope(db: web::Data<MySqlPool>, id: web::Path<i64>) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, difficulty, name_normalized,
               length_m, vertical_drop_m,
               CAST(average_gradient AS DOUBLE) AS average_gradient,
               CAST(max_gradient AS DOUBLE) AS max_gradient,
               snowmaking, night_skiing, family_friendly, race_slope,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               source_system, source_entity_id, operational_status, grooming_status, operational_note, status_source_url,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM slopes
        WHERE id = ?
        "#,
        *id
    )
    .fetch_optional(db.get_ref())
    .await;

    match result {
        Ok(Some(row)) => HttpResponse::Ok().json(Slope {
            id: row.id,
            resort_id: row.resort_id,
            name: row.name,
            display: SlopeDisplay {
                normalized_name: row.name_normalized,
                difficulty: row.difficulty,
            },
            geometry: SlopeGeometry {
                start: CoordinatePoint {
                    latitude: row.lat_start,
                    longitude: row.lon_start,
                },
                end: CoordinatePoint {
                    latitude: row.lat_end,
                    longitude: row.lon_end,
                },
            },
            specs: SlopeSpecs {
                length_m: row.length_m,
                vertical_drop_m: row.vertical_drop_m,
                average_gradient: row.average_gradient,
                max_gradient: row.max_gradient,
                snowmaking: row.snowmaking.unwrap_or(0) != 0,
                night_skiing: row.night_skiing.unwrap_or(0) != 0,
                family_friendly: row.family_friendly.unwrap_or(0) != 0,
                race_slope: row.race_slope.unwrap_or(0) != 0,
            },
            source: SlopeSource {
                system: row.source_system,
                entity_id: row.source_entity_id,
                source_url: row.status_source_url,
            },
            status: SlopeStatus {
                operational_status: row.operational_status,
                grooming_status: row.grooming_status,
                note: row.operational_note,
                updated_at: row.status_updated_at,
            },
        }),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => {
            eprintln!("GET /slopes/{{id}} error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_slopes_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, difficulty, name_normalized,
               length_m, vertical_drop_m,
               CAST(average_gradient AS DOUBLE) AS average_gradient,
               CAST(max_gradient AS DOUBLE) AS max_gradient,
               snowmaking, night_skiing, family_friendly, race_slope,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               source_system, source_entity_id, operational_status, grooming_status, operational_note, status_source_url,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM slopes
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
                .map(|row| Slope {
                    id: row.id,
                    resort_id: row.resort_id,
                    name: row.name,
                    display: SlopeDisplay {
                        normalized_name: row.name_normalized,
                        difficulty: row.difficulty,
                    },
                    geometry: SlopeGeometry {
                        start: CoordinatePoint {
                            latitude: row.lat_start,
                            longitude: row.lon_start,
                        },
                        end: CoordinatePoint {
                            latitude: row.lat_end,
                            longitude: row.lon_end,
                        },
                    },
                    specs: SlopeSpecs {
                        length_m: row.length_m,
                        vertical_drop_m: row.vertical_drop_m,
                        average_gradient: row.average_gradient,
                        max_gradient: row.max_gradient,
                        snowmaking: row.snowmaking.unwrap_or(0) != 0,
                        night_skiing: row.night_skiing.unwrap_or(0) != 0,
                        family_friendly: row.family_friendly.unwrap_or(0) != 0,
                        race_slope: row.race_slope.unwrap_or(0) != 0,
                    },
                    source: SlopeSource {
                        system: row.source_system,
                        entity_id: row.source_entity_id,
                        source_url: row.status_source_url,
                    },
                    status: SlopeStatus {
                        operational_status: row.operational_status,
                        grooming_status: row.grooming_status,
                        note: row.operational_note,
                        updated_at: row.status_updated_at,
                    },
                })
                .collect::<Vec<Slope>>(),
        ),
        Err(err) => {
            eprintln!("GET /slopes/by_resort error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn create_slope(
    db: web::Data<MySqlPool>,
    slope: web::Json<CreateSlope>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO slopes
        (resort_id, name, difficulty,
         length_m, vertical_drop_m, average_gradient, max_gradient,
         snowmaking, night_skiing, family_friendly, race_slope,
         lat_start, lon_start, lat_end, lon_end,
         source_system, source_entity_id, name_normalized,
         operational_status, grooming_status, operational_note, status_updated_at, status_source_url)
        VALUES (?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?)
        "#,
        slope.resort_id,
        slope.name,
        slope.difficulty,
        slope.length_m,
        slope.vertical_drop_m,
        slope.average_gradient,
        slope.max_gradient,
        slope.snowmaking.unwrap_or(false),
        slope.night_skiing.unwrap_or(false),
        slope.family_friendly.unwrap_or(false),
        slope.race_slope.unwrap_or(false),
        slope.lat_start,
        slope.lon_start,
        slope.lat_end,
        slope.lon_end,
        slope.source_system.as_deref().unwrap_or("osm"),
        slope.source_entity_id,
        slope.name_normalized,
        slope.operational_status.as_deref().unwrap_or("unknown"),
        slope.grooming_status.as_deref().unwrap_or("unknown"),
        slope.operational_note,
        slope.status_updated_at,
        slope.status_source_url
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

pub async fn update_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
    slope: web::Json<UpdateSlope>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE slopes
        SET resort_id = ?, name = ?, difficulty = ?,
            length_m = ?, vertical_drop_m = ?, average_gradient = ?, max_gradient = ?,
            snowmaking = ?, night_skiing = ?, family_friendly = ?, race_slope = ?,
            lat_start = ?, lon_start = ?, lat_end = ?, lon_end = ?,
            source_system = ?, source_entity_id = ?, name_normalized = ?,
            operational_status = ?, grooming_status = ?, operational_note = ?, status_updated_at = ?, status_source_url = ?
        WHERE id = ?
        "#,
        slope.resort_id,
        slope.name,
        slope.difficulty,
        slope.length_m,
        slope.vertical_drop_m,
        slope.average_gradient,
        slope.max_gradient,
        slope.snowmaking.unwrap_or(false),
        slope.night_skiing.unwrap_or(false),
        slope.family_friendly.unwrap_or(false),
        slope.race_slope.unwrap_or(false),
        slope.lat_start,
        slope.lon_start,
        slope.lat_end,
        slope.lon_end,
        slope.source_system.as_deref().unwrap_or("osm"),
        slope.source_entity_id,
        slope.name_normalized,
        slope.operational_status.as_deref().unwrap_or("unknown"),
        slope.grooming_status.as_deref().unwrap_or("unknown"),
        slope.operational_note,
        slope.status_updated_at,
        slope.status_source_url,
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

pub async fn delete_slope(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query!("DELETE FROM slopes WHERE id = ?", *id)
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

pub async fn delete_slopes_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!("DELETE FROM slopes WHERE resort_id = ?", resort_id.into_inner())
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
