// Simple migration runner
// Usage: cargo run --bin run_migrations

use sqlx::{PgPool, Row};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    println!("🔧 Connecting to database...");

    let pool = PgPool::connect(&database_url).await?;

    println!("✅ Connected to database");

    // Run migrations
    println!("📜 Running migrations...");

    // Check if transaction_program_ids table exists
    let table_exists = sqlx::query(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transaction_program_ids')"
    )
    .fetch_one(&pool)
    .await?;

    let exists: bool = table_exists.get(0);

    if !exists {
        println!("⚠️  transaction_program_ids table does not exist. Please run migration 005_normalize_program_ids.sql manually.");
        println!("   You can use psql or another PostgreSQL client to run the migrations in the migrations/ directory.");
    } else {
        println!("✅ transaction_program_ids table exists");
    }

    // Check if dashboard_reader user exists
    let user_exists = sqlx::query(
        "SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_reader')"
    )
    .fetch_one(&pool)
    .await?;

    let user_exists: bool = user_exists.get(0);

    if !user_exists {
        println!("⚠️  dashboard_reader user does not exist. Creating...");

        // Create dashboard_reader user
        sqlx::query(
            "CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'change_this_password_immediately'"
        )
        .execute(&pool)
        .await?;

        println!("✅ Created dashboard_reader user");

        // Grant permissions
        sqlx::query("GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_reader")
            .execute(&pool)
            .await?;

        sqlx::query("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_reader")
            .execute(&pool)
            .await?;

        sqlx::query("GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO dashboard_reader")
            .execute(&pool)
            .await?;

        println!("✅ Granted permissions to dashboard_reader");
    } else {
        println!("✅ dashboard_reader user exists");
    }

    println!("\n🎉 Migrations complete!");
    println!("\n💡 Next steps:");
    println!("   1. Change the dashboard_reader password: ALTER ROLE dashboard_reader WITH PASSWORD 'new_secure_password';");
    println!("   2. Set DASHBOARD_DATABASE_URL environment variable with the new password");
    println!("   3. Run the dashboard: cargo run --bin dashboard");

    Ok(())
}
