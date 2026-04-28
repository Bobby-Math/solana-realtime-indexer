// SQL Migration Runner
// Usage: cargo run --bin run_sql_migration -- <migration_number>

use sqlx::{PgPool, Row};
use std::env;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: cargo run --bin run_sql_migration -- <migration_number>");
        eprintln!("Example: cargo run --bin run_sql_migration -- 005");
        std::process::exit(1);
    }

    let migration_number = &args[1];
    let migration_file = format!("migrations/{}_{}.sql",
        migration_number,
        match migration_number.as_str() {
            "005" => "normalize_program_ids",
            "006" => "fix_log_messages_type",
            "007" => "dashboard_reader_user",
            _ => {
                eprintln!("Unknown migration number: {}", migration_number);
                eprintln!("Supported: 005, 006, 007");
                std::process::exit(1);
            }
        }
    );

    if !Path::new(&migration_file).exists() {
        eprintln!("Migration file not found: {}", migration_file);
        std::process::exit(1);
    }

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    println!("🔧 Connecting to database...");

    let pool = PgPool::connect(&database_url).await?;

    println!("✅ Connected to database");
    println!("📜 Running migration: {}", migration_file);

    // Read the migration SQL file
    let sql_content = std::fs::read_to_string(&migration_file)?;

    println!("📝 Read {} bytes from migration file", sql_content.len());

    // Split by semicolons and execute each statement
    // This is a simple approach - for production, use a proper migration library
    let statements: Vec<String> = sql_content
        .split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .collect();

    println!("🚧 Executing {} SQL statements...", statements.len());

    let force = args.contains(&"--force".to_string());

    for (i, statement) in statements.iter().enumerate() {
        if statement.is_empty() {
            continue;
        }

        println!("   [{}/{}] Executing statement...", i + 1, statements.len());

        match sqlx::query(statement).execute(&pool).await {
            Ok(result) => {
                println!("       ✅ Success (rows affected: {})", result.rows_affected());
            }
            Err(e) => {
                // Check if it's an "already exists" error
                let error_msg = e.to_string().to_lowercase();
                if error_msg.contains("already exists") ||
                   error_msg.contains("duplicate") ||
                   error_msg.contains("does not exist") && error_msg.contains("drop") {
                    println!("       ⚠️  Skipped (already applied or safe to ignore): {}", e);
                } else if force {
                    println!("       ⚠️  Forced continuation despite error: {}", e);
                } else {
                    eprintln!("       ❌ Error: {}", e);
                    eprintln!("       Statement: {}", &statement[..200.min(statement.len())]);
                    eprintln!("       Use --force to continue on errors");
                    return Err(e.into());
                }
            }
        }
    }

    println!("\n🎉 Migration {} complete!", migration_number);

    Ok(())
}
