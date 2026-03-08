use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Error, MySqlPool};
use std::collections::HashMap;

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
    pub official_website: Option<String>,
    pub lift_status_url: Option<String>,
    pub slope_status_url: Option<String>,
    pub snow_report_url: Option<String>,
    pub weather_url: Option<String>,
    pub status_provider: Option<String>,
    pub status_last_scraped_at: Option<String>,
    pub lifts_open_count: Option<i32>,
    pub slopes_open_count: Option<i32>,
    pub snow_depth_valley_cm: Option<i16>,
    pub snow_depth_mountain_cm: Option<i16>,
    pub new_snow_24h_cm: Option<i16>,
    pub temperature_valley_c: Option<f64>,
    pub temperature_mountain_c: Option<f64>,
}

#[derive(Deserialize)]
pub struct CreateResort {
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
    pub ski_area_type: String,
    pub official_website: Option<String>,
    pub lift_status_url: Option<String>,
    pub slope_status_url: Option<String>,
    pub snow_report_url: Option<String>,
    pub weather_url: Option<String>,
    pub status_provider: Option<String>,
    pub status_last_scraped_at: Option<String>,
    pub lifts_open_count: Option<i32>,
    pub slopes_open_count: Option<i32>,
    pub snow_depth_valley_cm: Option<i16>,
    pub snow_depth_mountain_cm: Option<i16>,
    pub new_snow_24h_cm: Option<i16>,
    pub temperature_valley_c: Option<f64>,
    pub temperature_mountain_c: Option<f64>,
}

#[derive(Deserialize)]
pub struct UpdateResort {
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
    pub ski_area_type: String,
    pub official_website: Option<String>,
    pub lift_status_url: Option<String>,
    pub slope_status_url: Option<String>,
    pub snow_report_url: Option<String>,
    pub weather_url: Option<String>,
    pub status_provider: Option<String>,
    pub status_last_scraped_at: Option<String>,
    pub lifts_open_count: Option<i32>,
    pub slopes_open_count: Option<i32>,
    pub snow_depth_valley_cm: Option<i16>,
    pub snow_depth_mountain_cm: Option<i16>,
    pub new_snow_24h_cm: Option<i16>,
    pub temperature_valley_c: Option<f64>,
    pub temperature_mountain_c: Option<f64>,
}

#[derive(Serialize, Clone)]
pub struct LiftSummary {
    pub id: i64,
    pub name: Option<String>,
    pub lift_type: String,
    pub geometry: LineGeometry,
    pub status: LiftStatusSummary,
}

#[derive(Serialize, Clone)]
pub struct SlopeSummary {
    pub id: i64,
    pub name: Option<String>,
    pub difficulty: String,
    pub geometry: LineGeometry,
    pub status: SlopeStatusSummary,
}

#[derive(Serialize, Clone)]
pub struct LineGeometry {
    pub start: CoordinatePoint,
    pub end: CoordinatePoint,
    pub path: Option<Vec<CoordinatePoint>>,
}

#[derive(Serialize, Clone)]
pub struct CoordinatePoint {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Serialize, Clone)]
pub struct LiftStatusSummary {
    pub operational_status: String,
    pub note: Option<String>,
    pub planned_open_time: Option<String>,
    pub planned_close_time: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct SlopeStatusSummary {
    pub operational_status: String,
    pub grooming_status: String,
    pub note: Option<String>,
    pub updated_at: Option<String>,
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
pub struct ResortSources {
    pub official_website: Option<String>,
    pub lift_status_url: Option<String>,
    pub slope_status_url: Option<String>,
    pub snow_report_url: Option<String>,
    pub weather_url: Option<String>,
    pub status_provider: Option<String>,
}

#[derive(Serialize)]
pub struct ResortLiveStatus {
    pub last_scraped_at: Option<String>,
    pub lifts_open_count: Option<i32>,
    pub slopes_open_count: Option<i32>,
    pub snow_depth_valley_cm: Option<i16>,
    pub snow_depth_mountain_cm: Option<i16>,
    pub new_snow_24h_cm: Option<i16>,
    pub temperature_valley_c: Option<f64>,
    pub temperature_mountain_c: Option<f64>,
}

#[derive(Serialize)]
pub struct ResortWithRelations {
    pub id: String,
    pub name: String,
    pub geography: Geography,
    pub altitude: Altitude,
    pub ski_area: SkiArea,
    pub sources: ResortSources,
    pub live_status: ResortLiveStatus,
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
            sources: ResortSources {
                official_website: resort.official_website,
                lift_status_url: resort.lift_status_url,
                slope_status_url: resort.slope_status_url,
                snow_report_url: resort.snow_report_url,
                weather_url: resort.weather_url,
                status_provider: resort.status_provider,
            },
            live_status: ResortLiveStatus {
                last_scraped_at: resort.status_last_scraped_at,
                lifts_open_count: resort.lifts_open_count,
                slopes_open_count: resort.slopes_open_count,
                snow_depth_valley_cm: resort.snow_depth_valley_cm,
                snow_depth_mountain_cm: resort.snow_depth_mountain_cm,
                new_snow_24h_cm: resort.new_snow_24h_cm,
                temperature_valley_c: resort.temperature_valley_c,
                temperature_mountain_c: resort.temperature_mountain_c,
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

fn parse_path_geojson(path_geojson: Option<String>) -> Option<Vec<CoordinatePoint>> {
    let raw = path_geojson?;
    let parsed: Value = serde_json::from_str(&raw).ok()?;
    let arr = parsed.as_array()?;

    let mut points: Vec<CoordinatePoint> = Vec::new();
    for item in arr {
        let latitude = item
            .get("latitude")
            .and_then(|v| v.as_f64())
            .or_else(|| item.get("lat").and_then(|v| v.as_f64()));
        let longitude = item
            .get("longitude")
            .and_then(|v| v.as_f64())
            .or_else(|| item.get("lon").and_then(|v| v.as_f64()));
        if latitude.is_none() || longitude.is_none() {
            continue;
        }
        points.push(CoordinatePoint {
            latitude,
            longitude,
        });
    }

    if points.is_empty() {
        None
    } else {
        Some(points)
    }
}

async fn load_lifts_by_resort(db: &MySqlPool) -> Result<HashMap<String, Vec<LiftSummary>>, Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id, resort_id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               operational_status, operational_note,
               DATE_FORMAT(planned_open_time, '%H:%i:%s') AS planned_open_time,
               DATE_FORMAT(planned_close_time, '%H:%i:%s') AS planned_close_time,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM lifts
        "#
    )
    .fetch_all(db)
    .await?;

    let mut map: HashMap<String, Vec<LiftSummary>> = HashMap::new();
    for row in rows {
        map.entry(row.resort_id.clone())
            .or_default()
            .push(LiftSummary {
                id: row.id,
                name: row.name,
                lift_type: row.lift_type,
                geometry: LineGeometry {
                    start: CoordinatePoint {
                        latitude: row.lat_start,
                        longitude: row.lon_start,
                    },
                    end: CoordinatePoint {
                        latitude: row.lat_end,
                        longitude: row.lon_end,
                    },
                    path: None,
                },
                status: LiftStatusSummary {
                    operational_status: row.operational_status,
                    note: row.operational_note,
                    planned_open_time: row.planned_open_time,
                    planned_close_time: row.planned_close_time,
                    updated_at: row.status_updated_at,
                },
            });
    }

    Ok(map)
}

async fn load_slopes_by_resort(db: &MySqlPool) -> Result<HashMap<String, Vec<SlopeSummary>>, Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id, resort_id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               CAST(path_geojson AS CHAR) AS path_geojson,
               operational_status, grooming_status, operational_note,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM slopes
        "#
    )
    .fetch_all(db)
    .await?;

    let mut map: HashMap<String, Vec<SlopeSummary>> = HashMap::new();
    for row in rows {
        map.entry(row.resort_id.clone())
            .or_default()
            .push(SlopeSummary {
                id: row.id,
                name: row.name,
                difficulty: row.difficulty,
                geometry: LineGeometry {
                    start: CoordinatePoint {
                        latitude: row.lat_start,
                        longitude: row.lon_start,
                    },
                    end: CoordinatePoint {
                        latitude: row.lat_end,
                        longitude: row.lon_end,
                    },
                    path: parse_path_geojson(row.path_geojson),
                },
                status: SlopeStatusSummary {
                    operational_status: row.operational_status,
                    grooming_status: row.grooming_status,
                    note: row.operational_note,
                    updated_at: row.status_updated_at,
                },
            });
    }

    Ok(map)
}

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
            ORDER BY name
            "#
        )
        .fetch_all(db.get_ref())
        .await;

        return match result {
            Ok(summaries) => HttpResponse::Ok().json(summaries),
            Err(_) => HttpResponse::InternalServerError().finish(),
        };
    }

    let resorts_result = sqlx::query!(
        r#"
        SELECT id, name, country, region, continent,
               CAST(latitude AS DOUBLE) AS latitude, CAST(longitude AS DOUBLE) AS longitude,
               village_altitude_m, min_altitude_m, max_altitude_m, ski_area_name, ski_area_type,
               official_website, lift_status_url, slope_status_url, snow_report_url, weather_url, status_provider,
               DATE_FORMAT(status_last_scraped_at, '%Y-%m-%dT%H:%i:%sZ') AS status_last_scraped_at,
               lifts_open_count, slopes_open_count,
               snow_depth_valley_cm, snow_depth_mountain_cm, new_snow_24h_cm,
               CAST(temperature_valley_c AS DOUBLE) AS temperature_valley_c,
               CAST(temperature_mountain_c AS DOUBLE) AS temperature_mountain_c
        FROM resorts
        ORDER BY name
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    let resorts = match resorts_result {
        Ok(rows) => rows
            .into_iter()
            .map(|row| Resort {
                id: row.id,
                name: row.name,
                country: row.country,
                region: row.region,
                continent: row.continent,
                latitude: row.latitude,
                longitude: row.longitude,
                village_altitude_m: row.village_altitude_m,
                min_altitude_m: row.min_altitude_m,
                max_altitude_m: row.max_altitude_m,
                ski_area_name: row.ski_area_name,
                ski_area_type: Some(row.ski_area_type),
                official_website: row.official_website,
                lift_status_url: row.lift_status_url,
                slope_status_url: row.slope_status_url,
                snow_report_url: row.snow_report_url,
                weather_url: row.weather_url,
                status_provider: row.status_provider,
                status_last_scraped_at: row.status_last_scraped_at,
                lifts_open_count: row.lifts_open_count,
                slopes_open_count: row.slopes_open_count,
                snow_depth_valley_cm: row.snow_depth_valley_cm,
                snow_depth_mountain_cm: row.snow_depth_mountain_cm,
                new_snow_24h_cm: row.new_snow_24h_cm,
                temperature_valley_c: row.temperature_valley_c,
                temperature_mountain_c: row.temperature_mountain_c,
            })
            .collect::<Vec<_>>(),
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
            let lifts = lifts_by_resort.get(&resort.id).cloned().unwrap_or_default();
            let slopes = slopes_by_resort.get(&resort.id).cloned().unwrap_or_default();
            ResortWithRelations::from_resort(resort, lifts, slopes)
        })
        .collect();

    HttpResponse::Ok().json(response)
}

pub async fn get_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
) -> impl Responder {
    let resort_result = sqlx::query!(
        r#"
        SELECT id, name, country, region, continent,
               CAST(latitude AS DOUBLE) AS latitude, CAST(longitude AS DOUBLE) AS longitude,
               village_altitude_m, min_altitude_m, max_altitude_m, ski_area_name, ski_area_type,
               official_website, lift_status_url, slope_status_url, snow_report_url, weather_url, status_provider,
               DATE_FORMAT(status_last_scraped_at, '%Y-%m-%dT%H:%i:%sZ') AS status_last_scraped_at,
               lifts_open_count, slopes_open_count,
               snow_depth_valley_cm, snow_depth_mountain_cm, new_snow_24h_cm,
               CAST(temperature_valley_c AS DOUBLE) AS temperature_valley_c,
               CAST(temperature_mountain_c AS DOUBLE) AS temperature_mountain_c
        FROM resorts
        WHERE id = ?
        "#,
        *id
    )
    .fetch_optional(db.get_ref())
    .await;

    let resort = match resort_result {
        Ok(Some(row)) => Resort {
            id: row.id,
            name: row.name,
            country: row.country,
            region: row.region,
            continent: row.continent,
            latitude: row.latitude,
            longitude: row.longitude,
            village_altitude_m: row.village_altitude_m,
            min_altitude_m: row.min_altitude_m,
            max_altitude_m: row.max_altitude_m,
            ski_area_name: row.ski_area_name,
            ski_area_type: Some(row.ski_area_type),
            official_website: row.official_website,
            lift_status_url: row.lift_status_url,
            slope_status_url: row.slope_status_url,
            snow_report_url: row.snow_report_url,
            weather_url: row.weather_url,
            status_provider: row.status_provider,
            status_last_scraped_at: row.status_last_scraped_at,
            lifts_open_count: row.lifts_open_count,
            slopes_open_count: row.slopes_open_count,
            snow_depth_valley_cm: row.snow_depth_valley_cm,
            snow_depth_mountain_cm: row.snow_depth_mountain_cm,
            new_snow_24h_cm: row.new_snow_24h_cm,
            temperature_valley_c: row.temperature_valley_c,
            temperature_mountain_c: row.temperature_mountain_c,
        },
        Ok(None) => return HttpResponse::NotFound().finish(),
        Err(Error::RowNotFound) => return HttpResponse::NotFound().finish(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let lifts_result = sqlx::query!(
        r#"
        SELECT id, name, lift_type,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               operational_status, operational_note,
               DATE_FORMAT(planned_open_time, '%H:%i:%s') AS planned_open_time,
               DATE_FORMAT(planned_close_time, '%H:%i:%s') AS planned_close_time,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM lifts
        WHERE resort_id = ?
        ORDER BY name
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
                name: row.name,
                lift_type: row.lift_type,
                geometry: LineGeometry {
                    start: CoordinatePoint {
                        latitude: row.lat_start,
                        longitude: row.lon_start,
                    },
                    end: CoordinatePoint {
                        latitude: row.lat_end,
                        longitude: row.lon_end,
                    },
                    path: None,
                },
                status: LiftStatusSummary {
                    operational_status: row.operational_status,
                    note: row.operational_note,
                    planned_open_time: row.planned_open_time,
                    planned_close_time: row.planned_close_time,
                    updated_at: row.status_updated_at,
                },
            })
            .collect(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let slopes_result = sqlx::query!(
        r#"
        SELECT id, name, difficulty,
               CAST(lat_start AS DOUBLE) AS lat_start, CAST(lon_start AS DOUBLE) AS lon_start,
               CAST(lat_end AS DOUBLE) AS lat_end, CAST(lon_end AS DOUBLE) AS lon_end,
               CAST(path_geojson AS CHAR) AS path_geojson,
               operational_status, grooming_status, operational_note,
               DATE_FORMAT(status_updated_at, '%Y-%m-%dT%H:%i:%sZ') AS status_updated_at
        FROM slopes
        WHERE resort_id = ?
        ORDER BY name
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
                name: row.name,
                difficulty: row.difficulty,
                geometry: LineGeometry {
                    start: CoordinatePoint {
                        latitude: row.lat_start,
                        longitude: row.lon_start,
                    },
                    end: CoordinatePoint {
                        latitude: row.lat_end,
                        longitude: row.lon_end,
                    },
                    path: parse_path_geojson(row.path_geojson),
                },
                status: SlopeStatusSummary {
                    operational_status: row.operational_status,
                    grooming_status: row.grooming_status,
                    note: row.operational_note,
                    updated_at: row.status_updated_at,
                },
            })
            .collect(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let response = ResortWithRelations::from_resort(resort, lifts, slopes);
    HttpResponse::Ok().json(response)
}

pub async fn create_resort(
    db: web::Data<MySqlPool>,
    resort: web::Json<CreateResort>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        INSERT INTO resorts
        (id, name, country, region, continent,
         latitude, longitude, village_altitude_m,
         min_altitude_m, max_altitude_m, ski_area_name, ski_area_type,
         official_website, lift_status_url, slope_status_url, snow_report_url, weather_url, status_provider,
         status_last_scraped_at, lifts_open_count, slopes_open_count,
         snow_depth_valley_cm, snow_depth_mountain_cm, new_snow_24h_cm,
         temperature_valley_c, temperature_mountain_c)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?)
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
        resort.ski_area_type,
        resort.official_website,
        resort.lift_status_url,
        resort.slope_status_url,
        resort.snow_report_url,
        resort.weather_url,
        resort.status_provider,
        resort.status_last_scraped_at,
        resort.lifts_open_count,
        resort.slopes_open_count,
        resort.snow_depth_valley_cm,
        resort.snow_depth_mountain_cm,
        resort.new_snow_24h_cm,
        resort.temperature_valley_c,
        resort.temperature_mountain_c
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(_) => HttpResponse::Created().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

pub async fn update_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
    resort: web::Json<UpdateResort>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        UPDATE resorts SET
            name = ?, country = ?, region = ?, continent = ?,
            latitude = ?, longitude = ?, village_altitude_m = ?,
            min_altitude_m = ?, max_altitude_m = ?, ski_area_name = ?, ski_area_type = ?,
            official_website = ?, lift_status_url = ?, slope_status_url = ?, snow_report_url = ?, weather_url = ?, status_provider = ?,
            status_last_scraped_at = ?, lifts_open_count = ?, slopes_open_count = ?,
            snow_depth_valley_cm = ?, snow_depth_mountain_cm = ?, new_snow_24h_cm = ?,
            temperature_valley_c = ?, temperature_mountain_c = ?
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
        resort.official_website,
        resort.lift_status_url,
        resort.slope_status_url,
        resort.snow_report_url,
        resort.weather_url,
        resort.status_provider,
        resort.status_last_scraped_at,
        resort.lifts_open_count,
        resort.slopes_open_count,
        resort.snow_depth_valley_cm,
        resort.snow_depth_mountain_cm,
        resort.new_snow_24h_cm,
        resort.temperature_valley_c,
        resort.temperature_mountain_c,
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

pub async fn delete_resort(
    db: web::Data<MySqlPool>,
    id: web::Path<String>,
) -> impl Responder {
    let result = sqlx::query!("DELETE FROM resorts WHERE id = ?", *id)
        .execute(db.get_ref())
        .await;

    match result {
        Ok(res) if res.rows_affected() == 0 => HttpResponse::NotFound().finish(),
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}
