use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

#[derive(Deserialize)]
pub struct StatusQuery {
    pub resort_id: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Serialize)]
pub struct ScrapeRunResponse {
    pub id: i64,
    pub resort_id: String,
    pub source_name: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub success: bool,
    pub http_status: Option<i32>,
    pub message: Option<String>,
}

#[derive(Serialize)]
pub struct ResortStatusSnapshotResponse {
    pub id: i64,
    pub run_id: i64,
    pub resort_id: String,
    pub snapshot_time: String,
    pub lifts: SnapshotMetric,
    pub slopes: SnapshotMetric,
    pub snow: SnowSnapshot,
    pub temperature: TemperatureSnapshot,
}

#[derive(Serialize)]
pub struct SnapshotMetric {
    pub open_count: Option<i32>,
    pub total_count: Option<i32>,
}

#[derive(Serialize)]
pub struct SnowSnapshot {
    pub valley_cm: Option<i16>,
    pub mountain_cm: Option<i16>,
    pub new_snow_24h_cm: Option<i16>,
}

#[derive(Serialize)]
pub struct TemperatureSnapshot {
    pub valley_c: Option<f64>,
    pub mountain_c: Option<f64>,
}

fn clamp_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 500)
}

pub async fn get_scrape_runs(
    db: web::Data<MySqlPool>,
    query: web::Query<StatusQuery>,
) -> impl Responder {
    let limit = clamp_limit(query.limit);
    let resort_id = query.resort_id.clone();

    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, source_name,
               DATE_FORMAT(started_at, '%Y-%m-%dT%H:%i:%sZ') AS started_at,
               DATE_FORMAT(finished_at, '%Y-%m-%dT%H:%i:%sZ') AS finished_at,
               success, http_status, message
        FROM scrape_runs
        WHERE (? IS NULL OR resort_id = ?)
        ORDER BY started_at DESC
        LIMIT ?
        "#,
        resort_id,
        resort_id,
        limit
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(rows) => {
            let response: Vec<ScrapeRunResponse> = rows
                .into_iter()
                .map(|row| ScrapeRunResponse {
                    id: row.id,
                    resort_id: row.resort_id,
                    source_name: row.source_name,
                    started_at: row.started_at.unwrap_or_else(|| "".to_string()),
                    finished_at: row.finished_at,
                    success: row.success != 0,
                    http_status: row.http_status,
                    message: row.message,
                })
                .collect();

            HttpResponse::Ok().json(response)
        }
        Err(err) => {
            eprintln!("GET /scrape-runs error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_scrape_run(
    db: web::Data<MySqlPool>,
    id: web::Path<i64>,
) -> impl Responder {
    let result = sqlx::query!(
        r#"
        SELECT id, resort_id, source_name,
               DATE_FORMAT(started_at, '%Y-%m-%dT%H:%i:%sZ') AS started_at,
               DATE_FORMAT(finished_at, '%Y-%m-%dT%H:%i:%sZ') AS finished_at,
               success, http_status, message
        FROM scrape_runs
        WHERE id = ?
        "#,
        *id
    )
    .fetch_optional(db.get_ref())
    .await;

    match result {
        Ok(Some(row)) => HttpResponse::Ok().json(ScrapeRunResponse {
            id: row.id,
            resort_id: row.resort_id,
            source_name: row.source_name,
            started_at: row.started_at.unwrap_or_else(|| "".to_string()),
            finished_at: row.finished_at,
            success: row.success != 0,
            http_status: row.http_status,
            message: row.message,
        }),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => {
            eprintln!("GET /scrape-runs/{{id}} error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_status_snapshots(
    db: web::Data<MySqlPool>,
    query: web::Query<StatusQuery>,
) -> impl Responder {
    let limit = clamp_limit(query.limit);
    let resort_id = query.resort_id.clone();

    let result = sqlx::query!(
        r#"
        SELECT id, run_id, resort_id,
               DATE_FORMAT(snapshot_time, '%Y-%m-%dT%H:%i:%sZ') AS snapshot_time,
               lifts_open_count, lifts_total_count,
               slopes_open_count, slopes_total_count,
               snow_depth_valley_cm, snow_depth_mountain_cm,
               new_snow_24h_cm,
               CAST(temperature_valley_c AS DOUBLE) AS temperature_valley_c,
               CAST(temperature_mountain_c AS DOUBLE) AS temperature_mountain_c
        FROM resort_status_snapshots
        WHERE (? IS NULL OR resort_id = ?)
        ORDER BY snapshot_time DESC
        LIMIT ?
        "#,
        resort_id,
        resort_id,
        limit
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(rows) => {
            let response: Vec<ResortStatusSnapshotResponse> = rows
                .into_iter()
                .map(|row| ResortStatusSnapshotResponse {
                    id: row.id,
                    run_id: row.run_id,
                    resort_id: row.resort_id,
                    snapshot_time: row.snapshot_time.unwrap_or_else(|| "".to_string()),
                    lifts: SnapshotMetric {
                        open_count: row.lifts_open_count,
                        total_count: row.lifts_total_count,
                    },
                    slopes: SnapshotMetric {
                        open_count: row.slopes_open_count,
                        total_count: row.slopes_total_count,
                    },
                    snow: SnowSnapshot {
                        valley_cm: row.snow_depth_valley_cm,
                        mountain_cm: row.snow_depth_mountain_cm,
                        new_snow_24h_cm: row.new_snow_24h_cm,
                    },
                    temperature: TemperatureSnapshot {
                        valley_c: row.temperature_valley_c,
                        mountain_c: row.temperature_mountain_c,
                    },
                })
                .collect();

            HttpResponse::Ok().json(response)
        }
        Err(err) => {
            eprintln!("GET /status-snapshots error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn get_status_snapshots_by_resort(
    db: web::Data<MySqlPool>,
    resort_id: web::Path<String>,
    query: web::Query<StatusQuery>,
) -> impl Responder {
    let limit = clamp_limit(query.limit);

    let result = sqlx::query!(
        r#"
        SELECT id, run_id, resort_id,
               DATE_FORMAT(snapshot_time, '%Y-%m-%dT%H:%i:%sZ') AS snapshot_time,
               lifts_open_count, lifts_total_count,
               slopes_open_count, slopes_total_count,
               snow_depth_valley_cm, snow_depth_mountain_cm,
               new_snow_24h_cm,
               CAST(temperature_valley_c AS DOUBLE) AS temperature_valley_c,
               CAST(temperature_mountain_c AS DOUBLE) AS temperature_mountain_c
        FROM resort_status_snapshots
        WHERE resort_id = ?
        ORDER BY snapshot_time DESC
        LIMIT ?
        "#,
        resort_id.into_inner(),
        limit
    )
    .fetch_all(db.get_ref())
    .await;

    match result {
        Ok(rows) => {
            let response: Vec<ResortStatusSnapshotResponse> = rows
                .into_iter()
                .map(|row| ResortStatusSnapshotResponse {
                    id: row.id,
                    run_id: row.run_id,
                    resort_id: row.resort_id,
                    snapshot_time: row.snapshot_time.unwrap_or_else(|| "".to_string()),
                    lifts: SnapshotMetric {
                        open_count: row.lifts_open_count,
                        total_count: row.lifts_total_count,
                    },
                    slopes: SnapshotMetric {
                        open_count: row.slopes_open_count,
                        total_count: row.slopes_total_count,
                    },
                    snow: SnowSnapshot {
                        valley_cm: row.snow_depth_valley_cm,
                        mountain_cm: row.snow_depth_mountain_cm,
                        new_snow_24h_cm: row.new_snow_24h_cm,
                    },
                    temperature: TemperatureSnapshot {
                        valley_c: row.temperature_valley_c,
                        mountain_c: row.temperature_mountain_c,
                    },
                })
                .collect();

            HttpResponse::Ok().json(response)
        }
        Err(err) => {
            eprintln!("GET /resorts/{{resort_id}}/status-snapshots error: {:?}", err);
            HttpResponse::InternalServerError().finish()
        }
    }
}
