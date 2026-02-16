use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use sqlx::{Error, MySqlPool};
use std::collections::HashMap;

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

#[derive(Serialize, Clone)]
pub struct LiftSummary {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub lift_type: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

#[derive(Serialize, Clone)]
pub struct SlopeSummary {
    pub id: i64,
    pub resort_id: String,
    pub name: Option<String>,
    pub difficulty: String,
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_end: Option<f64>,
    pub lon_end: Option<f64>,
}

#[derive(Serialize)]
pub struct Coordinates {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Serialize)]
pub struct Geography {
    pub continent: Option<String>,
    pub country: String,
    pub region: Option<String>,
    pub coordinates: Coordinates,
}

#[derive(Serialize)]
pub struct Altitude {
    pub village_m: Option<i32>,
    pub min_m: Option<i32>,
    pub max_m: Option<i32>,
}

#[derive(Serialize)]
pub struct SkiArea {
    pub name: Option<String>,
    pub area_type: Option<String>,
}

#[derive(Serialize)]
pub struct ResortWithRelations {
    pub id: String,
    pub name: String,
    pub geography: Geography,
    pub altitude: Altitude,
    pub ski_area: SkiArea,
    pub lifts: Vec<LiftSummary>,
    pub slopes: Vec<SlopeSummary>,
}

impl ResortWithRelations {
    fn from_resort(resort: Resort, lifts: Vec<LiftSummary>, slopes: Vec<SlopeSummary>) -> Self {
        Self {
            id: resort.id,
            name: resort.name,
            geography: Geography {
                continent: resort.continent,
                country: resort.country,
                region: resort.region,
                coordinates: Coordinates {
                    latitude: resort.latitude,
                    longitude: resort.longitude,
                },
            },
            altitude: Altitude {
                village_m: resort.village_altitude_m,
                min_m: resort.min_altitude_m,
                max_m: resort.max_altitude_m,
            },
            ski_area: SkiArea {
                name: resort.ski_area_name,
                area_type: resort.ski_area_type,
            },
            lifts,
            slopes,
        }
    }
}

#[derive(Deserialize)]
pub struct ResortsQuery {
    pub summary: Option<String>,
}

#[derive(Serialize)]
pub struct ResortSummary {
    pub id: String,
    pub name: String,
}

fn is_truthy_flag(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

/* ---------- HELPERS ---------- */

async fn load_lifts_by_resort(
    db: &MySqlPool,
) -> Result<HashMap<String, Vec<LiftSummary>>, Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id, resort_id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end
        FROM lifts
        "#
    )
    .fetch_all(db)
    .await?;

    let mut map: HashMap<String, Vec<LiftSummary>> = HashMap::new();

    for row in rows {
        let resort_id = row.resort_id.clone();
        map.entry(resort_id).or_default().push(LiftSummary {
            id: row.id,
            resort_id: row.resort_id,
            name: row.name,
            lift_type: row.lift_type,
            lat_start: row.lat_start,
            lon_start: row.lon_start,
            lat_end: row.lat_end,
            lon_end: row.lon_end,
        });
    }

    Ok(map)
}

async fn load_slopes_by_resort(
    db: &MySqlPool,
) -> Result<HashMap<String, Vec<SlopeSummary>>, Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id, resort_id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end
        FROM slopes
        "#
    )
    .fetch_all(db)
    .await?;

    let mut map: HashMap<String, Vec<SlopeSummary>> = HashMap::new();

    for row in rows {
        let resort_id = row.resort_id.clone();
        map.entry(resort_id).or_default().push(SlopeSummary {
            id: row.id,
            resort_id: row.resort_id,
            name: row.name,
            difficulty: row.difficulty,
            lat_start: row.lat_start,
            lon_start: row.lon_start,
            lat_end: row.lat_end,
            lon_end: row.lon_end,
        });
    }

    Ok(map)
}

/* ---------- HANDLER ---------- */

// GET /resorts
pub async fn get_resorts(
    db: web::Data<MySqlPool>,
    query: web::Query<ResortsQuery>,
) -> impl Responder {
    if query
        .summary
        .as_deref()
        .map(is_truthy_flag)
        .unwrap_or(false)
    {
        let result = sqlx::query_as!(
            ResortSummary,
            r#"
            SELECT id, name
            FROM resorts
            "#
        )
        .fetch_all(db.get_ref())
        .await;

        return match result {
            Ok(summaries) => HttpResponse::Ok().json(summaries),
            Err(_) => HttpResponse::InternalServerError().finish(),
        };
    }

    let resorts_result = sqlx::query_as!(
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

    let resorts = match resorts_result {
        Ok(data) => data,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let lifts_by_resort = match load_lifts_by_resort(db.get_ref()).await {
        Ok(data) => data,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let slopes_by_resort = match load_slopes_by_resort(db.get_ref()).await {
        Ok(data) => data,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let response: Vec<ResortWithRelations> = resorts
        .into_iter()
        .map(|resort| {
            let lifts = lifts_by_resort
                .get(&resort.id)
                .cloned()
                .unwrap_or_default();
            let slopes = slopes_by_resort
                .get(&resort.id)
                .cloned()
                .unwrap_or_default();

            ResortWithRelations::from_resort(resort, lifts, slopes)
        })
        .collect();

    HttpResponse::Ok().json(response)
}

// GET /resorts/{id}
pub async fn get_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
) -> impl Responder {
    let resort_result = sqlx::query_as!(
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

    let resort = match resort_result {
        Ok(data) => data,
        Err(Error::RowNotFound) => return HttpResponse::NotFound().finish(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let lifts_result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end
        FROM lifts
        WHERE resort_id = ?
        "#,
        resort.id
    )
    .fetch_all(db.get_ref())
    .await;

    let lifts = match lifts_result {
        Ok(rows) => rows
            .into_iter()
            .map(|row| LiftSummary {
                id: row.id,
                resort_id: row.resort_id,
                name: row.name,
                lift_type: row.lift_type,
                lat_start: row.lat_start,
                lon_start: row.lon_start,
                lat_end: row.lat_end,
                lon_end: row.lon_end,
            })
            .collect(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let slopes_result = sqlx::query!(
        r#"
        SELECT id, resort_id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end
        FROM slopes
        WHERE resort_id = ?
        "#,
        resort.id
    )
    .fetch_all(db.get_ref())
    .await;

    let slopes = match slopes_result {
        Ok(rows) => rows
            .into_iter()
            .map(|row| SlopeSummary {
                id: row.id,
                resort_id: row.resort_id,
                name: row.name,
                difficulty: row.difficulty,
                lat_start: row.lat_start,
                lon_start: row.lon_start,
                lat_end: row.lat_end,
                lon_end: row.lon_end,
            })
            .collect(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let response = ResortWithRelations::from_resort(resort, lifts, slopes);
    HttpResponse::Ok().json(response)
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
    let result = sqlx::query!("DELETE FROM resorts WHERE id = ?", *id)
        .execute(db.get_ref())
        .await;

    match result {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}


