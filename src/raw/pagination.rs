//! Endpoint-neutral cursor pagination primitives.
//!
//! The current contract uses two distinct, incompatible cursor styles:
//!
//! - most device/replicant/message/leaderboard list endpoints paginate with
//!   an opaque **integer** offset cursor ([`IntegerCursorPage`]);
//! - `GET /v1/events` and `GET /v1/inventory` paginate with an opaque
//!   **string** cursor ([`StringCursorPage`]), where for `/v1/events` the
//!   string is the event's own `id`.
//!
//! These are kept as separate types rather than one generic cursor so that a
//! caller cannot accidentally feed an integer offset into a string-cursor
//! endpoint or vice versa.

use std::{collections::HashSet, future::Future, pin::Pin};

use futures::{Stream, stream};

use crate::error::Error;

/// One page of results addressed by an opaque integer cursor.
#[derive(Clone, Debug)]
pub struct IntegerCursorPage<T> {
    /// Items contained in this page.
    pub items: Vec<T>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
}

/// One page of results addressed by an opaque string cursor.
#[derive(Clone, Debug)]
pub struct StringCursorPage<T> {
    /// Items contained in this page.
    pub items: Vec<T>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<String>,
}

/// Safeguards applied to every cursor paginator, so a server-side pagination
/// bug cannot turn into an unbounded local loop.
#[derive(Clone, Copy, Debug)]
pub struct PaginationBounds {
    /// Maximum number of pages to fetch before aborting.
    pub max_pages: usize,
    /// Maximum number of items to yield before aborting.
    pub max_items: usize,
}

impl Default for PaginationBounds {
    fn default() -> Self {
        Self {
            max_pages: 1_000,
            max_items: 100_000,
        }
    }
}

/// Pagination failures which are not themselves transport errors.
#[non_exhaustive]
#[derive(Debug)]
pub enum PaginationError {
    /// The same cursor was returned twice, indicating a server-side
    /// pagination loop.
    RepeatedCursor,
    /// The configured page or item bound was exceeded.
    BoundExceeded,
    /// The underlying page fetch failed.
    Transport(Error),
}

impl std::fmt::Display for PaginationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RepeatedCursor => write!(f, "pagination cursor repeated"),
            Self::BoundExceeded => write!(f, "pagination bound exceeded"),
            Self::Transport(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for PaginationError {}

impl From<Error> for PaginationError {
    fn from(error: Error) -> Self {
        Self::Transport(error)
    }
}

/// Creates a bounded stream over an integer-cursor paginated endpoint.
/// Dropping the stream cancels any pending fetch.
pub fn integer_cursor_stream<T, Fetch, Fut>(
    initial_cursor: Option<i64>,
    bounds: PaginationBounds,
    fetch: Fetch,
) -> Pin<Box<dyn Stream<Item = Result<T, PaginationError>> + Send>>
where
    T: Send + 'static,
    Fetch: Fn(Option<i64>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<IntegerCursorPage<T>, Error>> + Send + 'static,
{
    struct State<T, Fetch> {
        cursor: Option<i64>,
        seen: HashSet<i64>,
        page: usize,
        items: usize,
        pending: Vec<T>,
        fetch: Fetch,
        done: bool,
    }
    let state = State {
        cursor: initial_cursor,
        seen: HashSet::new(),
        page: 0,
        items: 0,
        pending: Vec::new(),
        fetch,
        done: false,
    };
    Box::pin(stream::try_unfold(state, move |mut state| async move {
        loop {
            if let Some(item) = state.pending.pop() {
                state.items += 1;
                if state.items > bounds.max_items {
                    return Err(PaginationError::BoundExceeded);
                }
                return Ok(Some((item, state)));
            }
            if state.done {
                return Ok(None);
            }
            if state.page >= bounds.max_pages {
                return Err(PaginationError::BoundExceeded);
            }
            if let Some(cursor) = state.cursor
                && !state.seen.insert(cursor)
            {
                return Err(PaginationError::RepeatedCursor);
            }
            let page = (state.fetch)(state.cursor).await?;
            state.page += 1;
            state.cursor = page.next_cursor;
            state.done = state.cursor.is_none();
            state.pending = page.items.into_iter().rev().collect();
        }
    }))
}

/// Creates a bounded stream over a string-cursor paginated endpoint.
/// Dropping the stream cancels any pending fetch.
pub fn string_cursor_stream<T, Fetch, Fut>(
    initial_cursor: Option<String>,
    bounds: PaginationBounds,
    fetch: Fetch,
) -> Pin<Box<dyn Stream<Item = Result<T, PaginationError>> + Send>>
where
    T: Send + 'static,
    Fetch: Fn(Option<String>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<StringCursorPage<T>, Error>> + Send + 'static,
{
    struct State<T, Fetch> {
        cursor: Option<String>,
        seen: HashSet<String>,
        page: usize,
        items: usize,
        pending: Vec<T>,
        fetch: Fetch,
        done: bool,
    }
    let state = State {
        cursor: initial_cursor,
        seen: HashSet::new(),
        page: 0,
        items: 0,
        pending: Vec::new(),
        fetch,
        done: false,
    };
    Box::pin(stream::try_unfold(state, move |mut state| async move {
        loop {
            if let Some(item) = state.pending.pop() {
                state.items += 1;
                if state.items > bounds.max_items {
                    return Err(PaginationError::BoundExceeded);
                }
                return Ok(Some((item, state)));
            }
            if state.done {
                return Ok(None);
            }
            if state.page >= bounds.max_pages {
                return Err(PaginationError::BoundExceeded);
            }
            if let Some(cursor) = &state.cursor
                && !state.seen.insert(cursor.clone())
            {
                return Err(PaginationError::RepeatedCursor);
            }
            let page = (state.fetch)(state.cursor.clone()).await?;
            state.page += 1;
            state.cursor = page.next_cursor;
            state.done = state.cursor.is_none();
            state.pending = page.items.into_iter().rev().collect();
        }
    }))
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::{
        IntegerCursorPage, PaginationBounds, PaginationError, StringCursorPage,
        integer_cursor_stream, string_cursor_stream,
    };

    #[tokio::test]
    async fn integer_cursor_repeated_is_rejected_before_an_unbounded_loop() {
        let stream = integer_cursor_stream(None, PaginationBounds::default(), |_| async {
            Ok(IntegerCursorPage {
                items: vec![1_u8],
                next_cursor: Some(7),
            })
        });
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(matches!(error, PaginationError::RepeatedCursor));
    }

    #[tokio::test]
    async fn integer_cursor_walks_pages_in_order() {
        let stream =
            integer_cursor_stream(Some(0), PaginationBounds::default(), |cursor| async move {
                match cursor {
                    Some(0) => Ok(IntegerCursorPage {
                        items: vec![1, 2],
                        next_cursor: Some(2),
                    }),
                    Some(2) => Ok(IntegerCursorPage {
                        items: vec![3],
                        next_cursor: None,
                    }),
                    other => panic!("unexpected cursor {other:?}"),
                }
            });
        let items = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn string_cursor_walks_pages_in_order() {
        let stream = string_cursor_stream(None, PaginationBounds::default(), |cursor| async move {
            match cursor.as_deref() {
                None => Ok(StringCursorPage {
                    items: vec!["a".to_string()],
                    next_cursor: Some("evt_2".to_string()),
                }),
                Some("evt_2") => Ok(StringCursorPage {
                    items: vec!["b".to_string()],
                    next_cursor: None,
                }),
                other => panic!("unexpected cursor {other:?}"),
            }
        });
        let items = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(items, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn string_cursor_repeated_is_rejected() {
        let stream = string_cursor_stream(None, PaginationBounds::default(), |_| async {
            Ok(StringCursorPage {
                items: vec![1_u8],
                next_cursor: Some("same".to_string()),
            })
        });
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(matches!(error, PaginationError::RepeatedCursor));
    }
}
