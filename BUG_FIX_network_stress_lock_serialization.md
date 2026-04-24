# Bug Fix: Network Stress Endpoint Lock Serialization

## Issue Summary

**Severity**: Performance bug (becomes critical under load)

**Location**: `src/api/network_stress.rs:27-106`

**Impact**: Global serialization of all API endpoints under concurrent load

## Problem Description

The `get_network_stress` endpoint held the `Arc<Mutex<ApiSnapshot>>` lock for the entire duration of a database query (potentially 10-100ms+). This caused all concurrent API requests (`/health`, `/metrics`, `/network_stress`) to block on the same lock, effectively serializing the entire API.

### Before Fix (Lines 27-106)

```rust
pub async fn get_network_stress(State(state): State<SharedApiState>) -> Json<NetworkStressResponse> {
    let snapshot = state.lock().await;  // ← Lock acquired

    let response = if let Some(pool) = snapshot.pool.as_ref() {
        let query_result = sqlx::query(...)
            .fetch_all(pool)  // ← DB query while holding lock!
            .await;
        // ... 50+ lines of processing ...
    };

    drop(snapshot); // Lock released here
    Json(response)
}
```

**Critical section span**: Entire DB round-trip + response processing (10-100ms+)

### After Fix

```rust
pub async fn get_network_stress(State(state): State<SharedApiState>) -> Json<NetworkStressResponse> {
    // Extract pool clone under lock, then release immediately
    let pool = {
        let snapshot = state.lock().await;
        snapshot.pool.clone()
    }; // ← Lock released here (after clone only)

    let response = if let Some(ref pool) = pool {
        let query_result = sqlx::query(...)
            .fetch_all(pool)  // ← DB query WITHOUT holding lock
            .await;
        // ... processing ...
    };

    Json(response)
}
```

**Critical section span**: Arc clone only (nanoseconds)

## Technical Details

### Why This Works

- `PgPool` internally uses `Arc` for cheap cloning
- Only the reference count is incremented under the lock
- DB queries execute concurrently without holding the API state lock
- Other endpoints (`/health`, `/metrics`) no longer block on DB queries

### Performance Impact

**Before**: Under load with 10 concurrent requests:
```
Request 1: acquires lock → DB query (50ms) → release
Request 2: waits 50ms → acquires → ...
Request 3: waits 50ms+10ms → acquires → ...
...
Total: ~500ms serialized latency
```

**After**:
```
Request 1: acquires lock → clone pool (0.001ms) → release → DB query (50ms)
Request 2: acquires lock → clone pool (0.001ms) → release → DB query (50ms)
Request 3: acquires lock → clone pool (0.001ms) → release → DB query (50ms)
...
Total: ~50ms concurrent latency
```

**10x improvement** under concurrent load.

## Testing

All 45 tests pass:
- 2 new tests for `network_stress` module
- 43 existing tests remain passing

```bash
$ cargo test --lib
test result: ok. 45 passed; 0 failed; 0 ignored
```

## Related Patterns

Check for similar anti-patterns in other API endpoints:
- ✅ `/health` - Only clones snapshot data, no DB queries
- ✅ `/metrics` - Only clones snapshot data, no DB queries
- ✅ `/network_stress` - Now fixed

## Files Changed

- `src/api/network_stress.rs`: Reduced critical section from ~100 lines to 3 lines

## Commit Message

```
fix: reduce lock scope in network_stress endpoint to prevent serialization

Extract PgPool clone under lock, then release immediately before DB query.
Previously held Arc<Mutex<ApiSnapshot>> for entire DB round-trip (10-100ms),
causing all API endpoints to serialize under load. Critical section reduced
from full query duration to Arc clone operation (nanoseconds).
```
