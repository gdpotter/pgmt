//! Loading a converted object kind the way a catalog load does.
//!
//! A converted kind needs the shared catalog state (namespace map, extension
//! ownership, descriptions, types) resolved on the *same* connection as its own
//! fetch, so tests go through this instead of each rolling its own preamble.

use anyhow::Result;
use pgmt::catalog::raw::shared::{self, SharedCatalog};
use sqlx::postgres::PgConnection;

pub async fn load_converted<T, F>(conn: &mut PgConnection, load: F) -> Result<Vec<T>>
where
    F: AsyncFnOnce(&mut PgConnection, &SharedCatalog) -> Result<Vec<T>>,
{
    let shared = shared::fetch(&mut *conn).await?;
    load(conn, &shared).await
}
