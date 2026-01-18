use actix_web::{web, HttpResponse};
use sqlx::MySqlPool;

use crate::models::db::SlopeRow;

pub async fn get_slope(
    pool: web::Data<MySqlPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, actix_web::Error> {

    let slope_id = path.into_inner();

    let slope = sqlx::query_as!(
        SlopeRow,
        r#"
        SELECT id, resort_id, name, difficulty
        FROM slopes
        WHERE id = ?
        "#,
        slope_id
    )
    .fetch_one(pool.get_ref())
    .await
    .map_err(|_| actix_web::error::ErrorNotFound("Slope not found"))?;

    let response = serde_json::json!({
        "slope": {
            "id": slope.id,
            "name": slope.name,
            "difficulty": slope.difficulty,
            "resort_id": slope.resort_id
        }
    });

    Ok(HttpResponse::Ok().json(response))
}