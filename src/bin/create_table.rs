// Simple table creation for testing
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    println!("🔧 Connecting to database...");

    let pool = PgPool::connect(&database_url).await?;

    println!("✅ Connected to database");

    // Create transaction_program_ids table
    println!("📜 Creating transaction_program_ids table...");

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS transaction_program_ids (
            timestamp TIMESTAMPTZ NOT NULL,
            signature BYTEA NOT NULL,
            program_id BYTEA NOT NULL,
            position SMALLINT NOT NULL,
            PRIMARY KEY (timestamp, signature, program_id, position)
        )
        "#
    )
    .execute(&pool)
    .await?;

    println!("✅ Created transaction_program_ids table");

    // Create index
    println!("📜 Creating index...");

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_transaction_program_ids_program_timestamp
            ON transaction_program_ids (program_id, timestamp DESC)
        "#
    )
    .execute(&pool)
    .await?;

    println!("✅ Created index");

    println!("🎉 Table creation complete!");
    println!("💡 Note: TimescaleDB hypertable conversion and compression policies should be configured manually");

    Ok(())
}
