use actix_web::{web, HttpResponse};
use sqlx::MySqlPool;

use crate::models::db::LiftRow;

pub async fn get_lift(
    pool: web::Data<MySqlPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, actix_web::Error> {

    let lift_id = path.into_inner();

    let lift = sqlx::query_as!(
        LiftRow,
        r#"
        SELECT id, resort_id, name, lift_type
        FROM lifts
        WHERE id = ?
        "#,
        lift_id
    )
    .fetch_one(pool.get_ref())
    .await
    .map_err(|_| actix_web::error::ErrorNotFound("Lift not found"))?;

    let response = serde_json::json!({
        "lift": {
            "id": lift.id,
            "name": lift.name,
            "type": lift.lift_type,
            "resort_id": lift.resort_id
        }
    });

    Ok(HttpResponse::Ok().json(response))
}