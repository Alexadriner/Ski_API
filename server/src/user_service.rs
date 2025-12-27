use sqlx::MySqlPool;
use crate::security::api_key::generate_api_key;
use crate::security::hash::hash_secret;

/// Erstellt einen neuen Benutzer
/// Gibt den API-Key im Klartext zurück (NUR EINMAL!)
pub async fn create_user(
    pool: &MySqlPool,
    name: &str,
    email: &str,
    password: &str,
) -> Result<String, sqlx::Error> {
    // 1. API-Key generieren
    let api_key_plain = generate_api_key();

    // 2. Hashes erzeugen
    let api_key_hash = hash_secret(&api_key_plain);
    let password_hash = hash_secret(password);

    // 3. User speichern
    sqlx::query!(
        r#"
        INSERT INTO users (name, email, password_hash, api_key, is_admin)
        VALUES (?, ?, ?, ?, 0)
        "#,
        name,
        email,
        password_hash,
        api_key_hash
    )
    .execute(pool)
    .await?;

    // 4. Klartext-API-Key zurückgeben
    Ok(api_key_plain)
}