//! Small, shared JSON:API transport and relationship primitives.
//!
//! FAF's data endpoints all use the same document envelope. Keeping that
//! envelope here prevents each feature client from quietly developing a
//! different parser or error policy.

use std::collections::HashMap;

use futures_util::StreamExt as _;
use serde::Deserialize;
use serde_json::Value;

use crate::infra::http::{describe_transport_error, REQUEST_DEADLINE};
use crate::ports::RequestError;

/// API documents should be measured in kilobytes or a few megabytes. A hard
/// ceiling prevents a broken proxy/server from turning `Response::text()` into
/// an unbounded allocation while still leaving ample room for relationship-
/// heavy vault pages.
const MAX_DOCUMENT_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct JsonApiDoc {
    /// Always a list here, whatever the server sent. JSON:API makes `data` an
    /// array for a collection and a single object for one resource, which is
    /// what every write echoes back: a created review came back as
    /// `{"data": {...}}`, failed to parse as a list, and the panel reported
    /// "invalid JSON" for a review the server had in fact saved.
    #[serde(default, deserialize_with = "one_or_many")]
    pub(crate) data: Vec<JsonApiResource>,
    #[serde(default)]
    pub(crate) included: Vec<JsonApiResource>,
    #[serde(default)]
    pub(crate) meta: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JsonApiResource {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) attributes: Value,
    #[serde(default)]
    pub(crate) relationships: Value,
}

/// `data` as a list, from either of the two shapes JSON:API allows, or from
/// `null` (a to-one lookup that found nothing).
fn one_or_many<'de, D>(deserializer: D) -> Result<Vec<JsonApiResource>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        Many(Vec<JsonApiResource>),
        One(Box<JsonApiResource>),
    }
    Ok(match Option::<OneOrMany>::deserialize(deserializer)? {
        Some(OneOrMany::Many(resources)) => resources,
        Some(OneOrMany::One(resource)) => vec![*resource],
        None => Vec::new(),
    })
}

pub(crate) type ResourceIndex<'a> = HashMap<(String, String), &'a JsonApiResource>;

/// How many pages a search has, from whichever of the two the server reported.
///
/// `page[totals]` is what asks for these, and the API does not always answer
/// with `totalPages`: the replay vault came back with neither, or with only a
/// record count, which left the pager in its "unknown total" mode showing
/// `Previous / Page 5 / Next` instead of numbered pages. Deriving the page count
/// from the record count when only that is present covers the difference, and
/// costs nothing when `totalPages` is there.
pub(crate) fn total_pages(meta: &Value, page_size: u32) -> Option<i32> {
    if let Some(pages) = meta_page_i32(meta, "totalPages").filter(|pages| *pages > 0) {
        return Some(pages);
    }
    let records = meta_page_i32(meta, "totalRecords").filter(|records| *records >= 0)?;
    let size = page_size.max(1) as i32;
    Some(((records + size - 1) / size).max(1))
}

pub(crate) fn meta_page_i32(meta: &Value, key: &str) -> Option<i32> {
    meta.get("page")?
        .get(key)?
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
}

/// How many catalogue pages to have in flight at once.
///
/// The vault crawls are the only place this client asks for tens of pages in a
/// row. Six is enough to hide almost all of the per-request latency without
/// turning one client's startup into a burst the API would notice.
const PAGE_FETCH_CONCURRENCY: usize = 6;

/// Fetch every page of a paginated collection.
///
/// The page count comes from the document's own `meta.page.totalPages`, so the
/// pages after the first are fetched together rather than one round trip at a
/// time. The catalogues this serves run to tens of pages, and walking them in
/// sequence made the wait the sum of every request instead of the slowest few.
///
/// Order is preserved (`buffered`, not `buffer_unordered`), because the caller's
/// `sort` is applied by the server across the whole collection and shuffling the
/// pages would quietly scramble it.
///
/// `max_pages` bounds the worst case if the collection ever grows huge, and a
/// server that reports no page count falls back to walking until a short page
/// arrives, which is what this did before.
pub(crate) async fn fetch_all_pages(
    http: &reqwest::Client,
    token: &str,
    max_pages: u32,
    page_size: usize,
    build_url: impl Fn(u32) -> Result<url::Url, String>,
) -> Result<Vec<JsonApiDoc>, String> {
    fetch_all_pages_with_progress(http, token, max_pages, page_size, |_, _| {}, build_url).await
}

pub(crate) async fn fetch_all_pages_with_progress(
    http: &reqwest::Client,
    token: &str,
    max_pages: u32,
    page_size: usize,
    report: impl Fn(u32, Option<u32>),
    build_url: impl Fn(u32) -> Result<url::Url, String>,
) -> Result<Vec<JsonApiDoc>, String> {
    let first = fetch_document(http, build_url(1)?, token).await?;
    let short_first_page = first.data.len() < page_size;
    let reported = meta_page_i32(&first.meta, "totalPages").filter(|pages| *pages > 0);
    let mut docs = vec![first];
    report(1, reported.map(|pages| pages as u32));

    let last = match reported {
        Some(pages) => u32::try_from(pages).unwrap_or(1).min(max_pages),
        // No page count: walk one at a time until a page comes back short.
        None => {
            if short_first_page {
                return Ok(docs);
            }
            for page in 2..=max_pages {
                let doc = fetch_document(http, build_url(page)?, token).await?;
                let short = doc.data.len() < page_size;
                docs.push(doc);
                report(page, None);
                if short {
                    break;
                }
            }
            return Ok(docs);
        }
    };

    if last > 1 {
        let mut rest = futures_util::stream::iter(2..=last)
            .map(|page| {
                let url = build_url(page);
                async move { fetch_document(http, url?, token).await }
            })
            .buffered(PAGE_FETCH_CONCURRENCY);
        while let Some(doc) = rest.next().await {
            docs.push(doc?);
            report(docs.len() as u32, reported.map(|pages| pages as u32));
        }
    }
    Ok(docs)
}

pub(crate) async fn fetch_document(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
) -> Result<JsonApiDoc, String> {
    fetch_document_typed(http, url, token)
        .await
        .map_err(|error| error.to_string())
}

/// Fetch one JSON:API document without erasing the recovery category.
///
/// Existing adapters keep using [`fetch_document`] until they migrate as a
/// complete vertical slice; this avoids a flag-day change across every port.
pub(crate) async fn fetch_document_typed(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
) -> Result<JsonApiDoc, RequestError> {
    fetch_document_within(http, url, token, REQUEST_DEADLINE).await
}

/// [`fetch_document_typed`] with the deadline as a parameter, so a test can
/// exercise the real path without waiting out the production one.
async fn fetch_document_within(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
    deadline: std::time::Duration,
) -> Result<JsonApiDoc, RequestError> {
    let response = http
        .get(url.clone())
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/vnd.api+json")
        .timeout(deadline)
        .send()
        .await
        .map_err(request_error)?;
    let status = response.status();
    let body = bounded_document_body(response).await?;
    if !status.is_success() {
        return Err(response_error(status, url.path(), &body));
    }
    serde_json::from_str(&body)
        .map_err(|error| RequestError::unexpected(format!("invalid server response: {error}")))
}

pub(crate) fn request_error(error: reqwest::Error) -> RequestError {
    // Checked first: a timeout is often also a connect or body error, and
    // "could not reach" is the wrong thing to say about a server that was
    // reached and then went quiet.
    if error.is_timeout() {
        RequestError::offline(
            "The request to FAF services timed out. Check your connection and try again.",
        )
    } else if error.is_connect() || error.is_body() {
        RequestError::offline("Could not reach FAF services. Check your connection and try again.")
    } else {
        RequestError::unexpected(format!("request could not be completed: {error}"))
    }
}

/// A request that never got an answer, for the writes that report text.
fn failed_request(error: reqwest::Error) -> String {
    format!("request failed: {}", describe_transport_error(&error))
}

/// A failed write, as the category the caller can act on.
///
/// The same reading as a failed read: they differ only in which verb produced
/// the status, and a 403 on a clan edit means what a 403 on a clan read means.
pub(crate) fn write_error(status: reqwest::StatusCode, path: &str, body: &str) -> RequestError {
    response_error(status, path, body)
}

fn response_error(status: reqwest::StatusCode, path: &str, body: &str) -> RequestError {
    let detail = api_error_detail(body);
    match status {
        reqwest::StatusCode::UNAUTHORIZED => {
            RequestError::unauthorized("Your FAF session has expired. Sign out and sign in again.")
        }
        reqwest::StatusCode::NOT_FOUND => {
            RequestError::not_found(format!("The FAF resource at {path} was not found."))
        }
        status if status.is_server_error() => RequestError::offline(
            "FAF services are temporarily unavailable. Please try again shortly.",
        ),
        status if status.is_client_error() => RequestError::rejected(
            detail.unwrap_or_else(|| format!("FAF rejected the request ({status}).")),
        ),
        _ => RequestError::unexpected(format!("FAF returned an unexpected status ({status}).")),
    }
}

/// The JSON:API media type. Required on writes: the API rejects a body sent
/// as `application/json`.
const MEDIA_TYPE: &str = "application/vnd.api+json";

/// Create a resource, returning the document the server echoed back.
///
/// The first *write* path in this client; every other API client here reads.
/// `attributes` is the resource's own fields: the caller supplies the type
/// and the URL decides the parent. `relationships` is any other link the
/// resource needs, such as a review's author; `Value::Null` sends none.
pub(crate) async fn post_resource(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
    resource_type: &str,
    attributes: Value,
    relationships: Value,
) -> Result<JsonApiDoc, String> {
    let mut data = serde_json::json!({ "type": resource_type, "attributes": attributes });
    if !relationships.is_null() {
        data["relationships"] = relationships;
    }
    let body = serde_json::json!({ "data": data });
    let response = http
        .post(url.clone())
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, MEDIA_TYPE)
        .header(reqwest::header::ACCEPT, MEDIA_TYPE)
        .json(&body)
        .timeout(REQUEST_DEADLINE)
        .send()
        .await
        .map_err(failed_request)?;
    write_response(url.path(), response).await
}

/// Update a resource in place. JSON:API requires the id inside the body as
/// well as in the URL, and the server rejects the request if they disagree.
pub(crate) async fn patch_resource(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
    resource_type: &str,
    id: &str,
    attributes: Value,
) -> Result<(), String> {
    let body = serde_json::json!({
        "data": { "type": resource_type, "id": id, "attributes": attributes },
    });
    let response = http
        .patch(url.clone())
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, MEDIA_TYPE)
        .header(reqwest::header::ACCEPT, MEDIA_TYPE)
        .json(&body)
        .timeout(REQUEST_DEADLINE)
        .send()
        .await
        .map_err(failed_request)?;
    write_response(url.path(), response).await.map(|_| ())
}

/// Update a resource with a caller-supplied `data` object.
///
/// The counterpart of [`patch_resource`] for a write that changes a
/// *relationship* rather than an attribute: handing a clan to a new leader is
/// `relationships.leader`, which an attributes-only body cannot express.
///
/// Typed failures, because the categories are the point of these writes: a 403
/// means somebody else is the leader now, and a 409 means the change collided
/// with one made elsewhere.
pub(crate) async fn patch_document(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
    data: Value,
) -> Result<(), RequestError> {
    let response = http
        .patch(url.clone())
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, MEDIA_TYPE)
        .header(reqwest::header::ACCEPT, MEDIA_TYPE)
        .json(&serde_json::json!({ "data": data }))
        .timeout(REQUEST_DEADLINE)
        .send()
        .await
        .map_err(request_error)?;
    typed_write_response(url.path(), response).await
}

/// Delete a resource, keeping the failure's category.
pub(crate) async fn delete_resource_typed(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
) -> Result<(), RequestError> {
    let response = http
        .delete(url.clone())
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, MEDIA_TYPE)
        .timeout(REQUEST_DEADLINE)
        .send()
        .await
        .map_err(request_error)?;
    typed_write_response(url.path(), response).await
}

/// A write's outcome, keeping both the server's sentence and its category.
async fn typed_write_response(path: &str, response: reqwest::Response) -> Result<(), RequestError> {
    let status = response.status();
    let body = bounded_document_body(response).await?;
    if !status.is_success() {
        return Err(write_error(status, path, &body));
    }
    Ok(())
}

pub(crate) async fn delete_resource(
    http: &reqwest::Client,
    url: url::Url,
    token: &str,
) -> Result<(), String> {
    let response = http
        .delete(url.clone())
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, MEDIA_TYPE)
        .timeout(REQUEST_DEADLINE)
        .send()
        .await
        .map_err(failed_request)?;
    write_response(url.path(), response).await.map(|_| ())
}

/// Interpret a write response.
///
/// A successful write may legitimately return no body (`204`), so an empty
/// response is an empty document rather than a parse error. A failure carries
/// JSON:API `errors`, which say far more than the status code: "you already
/// reviewed this" instead of "422".
async fn write_response(path: &str, response: reqwest::Response) -> Result<JsonApiDoc, String> {
    let status = response.status();
    let body = bounded_document_body(response)
        .await
        .map_err(|error| error.to_string())?;

    if !status.is_success() {
        return Err(match api_error_detail(&body) {
            Some(detail) => detail,
            None => format!(
                "{path} returned {status}: {}",
                body.chars().take(240).collect::<String>()
            ),
        });
    }
    if body.trim().is_empty() {
        return Ok(JsonApiDoc::default());
    }
    serde_json::from_str(&body).map_err(|error| format!("invalid JSON: {error}"))
}

pub(crate) async fn bounded_document_body(
    response: reqwest::Response,
) -> Result<String, RequestError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_DOCUMENT_BYTES)
    {
        return Err(RequestError::unexpected(
            "FAF returned a response that is too large to process safely.",
        ));
    }

    let mut bytes = Vec::new();
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(request_error)?;
        received = received
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| RequestError::unexpected("FAF returned an oversized response."))?;
        if received > MAX_DOCUMENT_BYTES {
            return Err(RequestError::unexpected(
                "FAF returned a response that is too large to process safely.",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }

    String::from_utf8(bytes)
        .map_err(|_| RequestError::unexpected("FAF returned a response that was not valid UTF-8."))
}

/// Pull the human-readable half out of a JSON:API error document.
pub(crate) fn api_error_detail(body: &str) -> Option<String> {
    let value: Value = serde_json::from_str(body).ok()?;
    let errors = value.get("errors")?.as_array()?;
    let messages: Vec<String> = errors
        .iter()
        .filter_map(|error| {
            error
                .get("detail")
                .or_else(|| error.get("title"))
                .and_then(Value::as_str)
        })
        .map(str::to_string)
        .collect();
    (!messages.is_empty()).then(|| messages.join("; "))
}

pub(crate) fn resource_index(included: &[JsonApiResource]) -> ResourceIndex<'_> {
    included
        .iter()
        .map(|resource| ((resource.kind.clone(), resource.id.clone()), resource))
        .collect()
}

pub(crate) fn document_index(doc: &JsonApiDoc) -> ResourceIndex<'_> {
    resource_index(&doc.included)
}

pub(crate) fn rel_target(relationships: &Value, name: &str) -> Option<(String, String)> {
    let data = relationships.get(name)?.get("data")?;
    if data.is_null() {
        return None;
    }
    Some((
        data.get("type")?.as_str()?.to_string(),
        data.get("id")?.as_str()?.to_string(),
    ))
}

pub(crate) fn rel_targets(relationships: &Value, name: &str) -> Vec<(String, String)> {
    relationships
        .get(name)
        .and_then(|relationship| relationship.get("data"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            Some((
                value.get("type")?.as_str()?.to_string(),
                value.get("id")?.as_str()?.to_string(),
            ))
        })
        .collect()
}

pub(crate) fn rel_one(resource: &JsonApiResource, name: &str) -> Option<(String, String)> {
    rel_target(&resource.relationships, name)
}

// ── Attribute readers ────────────────────────────────────────────────────────
//
// FAF's API is not consistently typed: the same logical field arrives as a JSON
// number from one endpoint and a quoted string from another, and Challonge's
// proxied objects add a third shape again. Each feature client grew its own
// copy of these, and the copies had *already* drifted: maps and mods accepted
// floats but not strings, tournaments accepted strings but not floats, and only
// maps read a number back as a string.
//
// These are the union of what those copies tolerated. Being more permissive
// than any single previous reader is the safe direction: the alternative to
// parsing a `"24"` is silently reporting zero.

/// An integer attribute, from a JSON number, a float (rounded), or a numeric
/// string. `None` when absent, null, or unparseable.
pub(crate) fn value_i32(attributes: &Value, name: &str) -> Option<i32> {
    let value = attributes.get(name)?;
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .or_else(|| {
            value.as_f64().and_then(|number| {
                let rounded = number.round();
                (rounded.is_finite()
                    && rounded >= f64::from(i32::MIN)
                    && rounded <= f64::from(i32::MAX))
                .then_some(rounded as i32)
            })
        })
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
}

/// A string attribute, rendering a numeric one as text. Empty when absent.
pub(crate) fn value_string(attributes: &Value, name: &str) -> String {
    attributes
        .get(name)
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

/// A boolean attribute, accepting the `0`/`1` some endpoints send. `false` when
/// absent: every caller treats a missing flag as not set.
pub(crate) fn value_bool(attributes: &Value, name: &str) -> bool {
    attributes
        .get(name)
        .and_then(|value| {
            value
                .as_bool()
                .or_else(|| value.as_i64().map(|number| number != 0))
        })
        .unwrap_or(false)
}

/// A floating-point attribute, from a JSON number or numeric string.
/// `None` when absent, null, or unparseable.
pub(crate) fn value_f64(attributes: &Value, name: &str) -> Option<f64> {
    let value = attributes.get(name)?;
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
}

/// Look up a relationship resource in index by `(kind, id)`, falling back to
/// matching by `id` alone in `doc.included` if the relationship type differs.
pub(crate) fn find_rel_resource<'a>(
    doc: &'a JsonApiDoc,
    index: &ResourceIndex<'a>,
    rel: Option<(String, String)>,
) -> Option<&'a JsonApiResource> {
    let (kind, id) = rel?;
    if let Some(res) = index.get(&(kind, id.clone())) {
        return Some(*res);
    }
    doc.included.iter().find(|r| r.id == id)
}

pub(crate) fn rel_many(resource: &JsonApiResource, name: &str) -> Vec<(String, String)> {
    rel_targets(&resource.relationships, name)
}

#[cfg(test)]
mod total_pages_tests {
    use super::*;

    fn meta(json: &str) -> Value {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_reported_page_count_is_used_as_is() {
        assert_eq!(
            total_pages(&meta(r#"{"page":{"totalPages":7,"totalRecords":250}}"#), 36),
            Some(7)
        );
    }

    /// The case that left the replay pager showing `Page 5` with no numbers.
    #[test]
    fn a_record_count_alone_still_yields_a_page_count() {
        assert_eq!(
            total_pages(&meta(r#"{"page":{"totalRecords":131}}"#), 36),
            Some(4)
        );
        assert_eq!(
            total_pages(&meta(r#"{"page":{"totalRecords":36}}"#), 36),
            Some(1)
        );
    }

    /// No results is still one page: a pager that claims zero pages has nothing
    /// to render and no way back.
    #[test]
    fn an_empty_result_is_one_page() {
        assert_eq!(
            total_pages(&meta(r#"{"page":{"totalRecords":0}}"#), 36),
            Some(1)
        );
    }

    /// Neither reported: the pager keeps its unknown-total mode rather than
    /// inventing a number.
    #[test]
    fn nothing_reported_stays_unknown() {
        assert_eq!(total_pages(&meta(r#"{"page":{}}"#), 36), None);
        assert_eq!(total_pages(&meta("{}"), 36), None);
    }
}

#[cfg(test)]
mod paging_tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;

    /// A minimal JSON:API server: one resource list per page, optionally
    /// reporting `meta.page.totalPages`. Returns its base URL and the pages it
    /// was asked for.
    async fn serve(
        page_size: usize,
        pages: usize,
        report_total: bool,
    ) -> (String, Arc<Mutex<Vec<u32>>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let requested = Arc::new(Mutex::new(Vec::new()));
        let seen = requested.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let read = stream.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..read]).to_string();
                    let target = request
                        .lines()
                        .next()
                        .and_then(|line| line.split(' ').nth(1))
                        .unwrap_or("")
                        .to_string();
                    let page: u32 = target
                        .split(['?', '&'])
                        .find_map(|pair| pair.strip_prefix("page%5Bnumber%5D="))
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(1);
                    seen.lock().unwrap().push(page);

                    let count = if page as usize == pages {
                        page_size - 1 // a short final page
                    } else if page as usize > pages {
                        0
                    } else {
                        page_size
                    };
                    let data: Vec<String> = (0..count)
                        .map(|index| format!(r#"{{"type":"map","id":"{page}-{index}"}}"#))
                        .collect();
                    let meta = if report_total {
                        format!(r#","meta":{{"page":{{"totalPages":{pages}}}}}"#)
                    } else {
                        String::new()
                    };
                    let body = format!(r#"{{"data":[{}]{meta}}}"#, data.join(","));
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        (base, requested)
    }

    fn url_builder(base: String) -> impl Fn(u32) -> Result<url::Url, String> {
        move |page| {
            let mut url =
                url::Url::parse(&format!("{base}/data/map")).map_err(|e| e.to_string())?;
            url.query_pairs_mut()
                .append_pair("page[size]", "10")
                .append_pair("page[number]", &page.to_string());
            Ok(url)
        }
    }

    /// The pages after the first go out together, and the caller still sees
    /// them in order: the server sorts across the whole collection, so a
    /// shuffled result would silently scramble "newest first".
    #[tokio::test]
    async fn reported_page_counts_are_fetched_together_and_stay_in_order() {
        let (base, requested) = serve(10, 4, true).await;
        let docs = fetch_all_pages(&reqwest::Client::new(), "token", 50, 10, url_builder(base))
            .await
            .unwrap();

        assert_eq!(docs.len(), 4);
        let first_ids: Vec<&str> = docs
            .iter()
            .map(|doc| doc.data.first().unwrap().id.as_str())
            .collect();
        assert_eq!(first_ids, ["1-0", "2-0", "3-0", "4-0"]);

        let mut pages = requested.lock().unwrap().clone();
        pages.sort_unstable();
        assert_eq!(
            pages,
            [1, 2, 3, 4],
            "no page fetched twice, none probed past the end"
        );
    }

    /// Without a page count there is nothing to plan against, so it walks until
    /// a short page arrives, exactly as this did before.
    #[tokio::test]
    async fn a_server_without_a_page_count_is_walked_one_page_at_a_time() {
        let (base, requested) = serve(10, 3, false).await;
        let docs = fetch_all_pages(&reqwest::Client::new(), "token", 50, 10, url_builder(base))
            .await
            .unwrap();

        assert_eq!(docs.len(), 3);
        assert_eq!(*requested.lock().unwrap(), [1, 2, 3]);
    }

    #[tokio::test]
    async fn a_single_short_page_is_not_followed_by_another_request() {
        let (base, requested) = serve(10, 1, false).await;
        let docs = fetch_all_pages(&reqwest::Client::new(), "token", 50, 10, url_builder(base))
            .await
            .unwrap();

        assert_eq!(docs.len(), 1);
        assert_eq!(*requested.lock().unwrap(), [1]);
    }

    /// The cap is what stops a catalogue that outgrows it from being crawled
    /// forever; it truncates rather than failing.
    #[tokio::test]
    async fn the_page_cap_bounds_the_crawl() {
        let (base, requested) = serve(10, 40, true).await;
        let docs = fetch_all_pages(&reqwest::Client::new(), "token", 3, 10, url_builder(base))
            .await
            .unwrap();

        assert_eq!(docs.len(), 3);
        assert_eq!(requested.lock().unwrap().len(), 3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_resource_document_parses_as_a_one_item_list() {
        // What a POST or PATCH echoes back: the resource it wrote, as an
        // object rather than an array.
        let doc: JsonApiDoc = serde_json::from_str(
            r#"{"data":{"type":"gameReview","id":"12","attributes":{"score":4,"text":"gg"}}}"#,
        )
        .expect("a single-resource document");
        assert_eq!(doc.data.len(), 1);
        assert_eq!(doc.data[0].kind, "gameReview");
        assert_eq!(doc.data[0].id, "12");

        let empty: JsonApiDoc = serde_json::from_str(r#"{"data":null}"#).expect("null data");
        assert!(empty.data.is_empty());
        let list: JsonApiDoc = serde_json::from_str(r#"{"data":[]}"#).expect("an empty list");
        assert!(list.data.is_empty());
    }

    #[test]
    fn indexes_included_resources_and_reads_relationships() {
        let doc: JsonApiDoc = serde_json::from_str(
            r#"{
                "data": [],
                "included": [{
                    "type": "player",
                    "id": "7",
                    "attributes": { "login": "Example" },
                    "relationships": {
                        "clan": { "data": { "type": "clan", "id": "2" } },
                        "games": { "data": [
                            { "type": "game", "id": "3" },
                            { "type": "game", "id": "4" }
                        ] }
                    }
                }]
            }"#,
        )
        .expect("valid document");

        let index = document_index(&doc);
        let player = index
            .get(&("player".to_string(), "7".to_string()))
            .expect("player indexed");
        assert_eq!(rel_one(player, "clan"), Some(("clan".into(), "2".into())));
        assert_eq!(rel_many(player, "games").len(), 2);
    }

    #[test]
    fn an_integer_is_read_from_every_shape_the_api_sends() {
        let attributes = serde_json::json!({
            "number": 24,
            "float": 23.6,
            "tooLarge": 2147483648_i64,
            "tooSmall": -2147483649_i64,
            "text": "24",
            "padded": " 24 ",
            "nothing": null,
            "junk": "many",
        });
        assert_eq!(value_i32(&attributes, "number"), Some(24));
        assert_eq!(value_i32(&attributes, "float"), Some(24), "rounded");
        assert_eq!(value_i32(&attributes, "tooLarge"), None, "must not wrap");
        assert_eq!(value_i32(&attributes, "tooSmall"), None, "must not wrap");
        assert_eq!(value_i32(&attributes, "text"), Some(24));
        assert_eq!(value_i32(&attributes, "padded"), Some(24));
        assert_eq!(value_i32(&attributes, "nothing"), None);
        assert_eq!(value_i32(&attributes, "junk"), None);
        assert_eq!(value_i32(&attributes, "absent"), None);
    }

    #[test]
    fn a_string_falls_back_to_rendering_a_number() {
        // A map or mod whose name is all digits arrives unquoted from some
        // endpoints; returning "" for it would blank the row.
        let attributes = serde_json::json!({ "text": "Seton's", "number": 1234, "float": 12.5 });
        assert_eq!(value_string(&attributes, "text"), "Seton's");
        assert_eq!(value_string(&attributes, "number"), "1234");
        assert_eq!(value_string(&attributes, "float"), "12.5");
        assert_eq!(value_string(&attributes, "absent"), "");
    }

    #[test]
    fn a_boolean_accepts_the_zero_one_form() {
        let attributes = serde_json::json!({ "yes": true, "one": 1, "zero": 0 });
        assert!(value_bool(&attributes, "yes"));
        assert!(value_bool(&attributes, "one"));
        assert!(!value_bool(&attributes, "zero"));
        assert!(!value_bool(&attributes, "absent"));
    }

    #[test]
    fn read_failures_keep_the_recovery_category() {
        use faf_domain::state::RequestFailureKind;
        use reqwest::StatusCode;

        let cases = [
            (StatusCode::UNAUTHORIZED, RequestFailureKind::Unauthorized),
            (StatusCode::NOT_FOUND, RequestFailureKind::NotFound),
            (StatusCode::FORBIDDEN, RequestFailureKind::Rejected),
            (StatusCode::SERVICE_UNAVAILABLE, RequestFailureKind::Offline),
            (StatusCode::FOUND, RequestFailureKind::Unexpected),
        ];

        for (status, expected) in cases {
            let error = response_error(status, "/data/coopMission", "not JSON");
            assert_eq!(error.kind(), expected, "status {status}");
        }
    }

    #[test]
    fn a_rejection_preserves_the_json_api_explanation() {
        let error = response_error(
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            "/data/coopMission",
            r#"{"errors":[{"detail":"That filter is not allowed."}]}"#,
        );
        assert_eq!(error.message(), "That filter is not allowed.");
    }

    #[test]
    fn server_errors_do_not_echo_an_untrusted_response_body() {
        let error = response_error(
            reqwest::StatusCode::BAD_GATEWAY,
            "/data/coopMission",
            "proxy dump containing internal details",
        );
        assert!(!error.message().contains("internal details"));
    }

    /// A server that accepts the connection, optionally sends `prefix`, and
    /// then goes quiet with the socket held open until the test ends.
    async fn silent_server(prefix: &'static [u8]) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await;
            let _ = socket.write_all(prefix).await;
            std::future::pending::<()>().await;
        });
        address
    }

    async fn fetch_from_silent_server(prefix: &'static [u8]) -> RequestError {
        let address = silent_server(prefix).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fetch_document_within(
                &reqwest::Client::new(),
                url::Url::parse(&format!("http://{address}/data/test")).expect("test URL"),
                "token",
                std::time::Duration::from_millis(200),
            ),
        )
        .await
        .expect("the request deadline fires well before the test's own")
        .expect_err("a silent server must fail")
    }

    /// The case that held a request, and its caller's lock, until restart: a
    /// connected server that stops answering. The deadline covers both the
    /// wait for an answer and the document body, and its expiry reads as a
    /// timeout rather than as "could not reach".
    #[tokio::test]
    async fn a_server_that_stops_answering_hits_the_deadline_as_a_timeout() {
        use faf_domain::state::RequestFailureKind;

        for prefix in [
            b"".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{\"data\":".as_slice(),
        ] {
            let error = fetch_from_silent_server(prefix).await;
            assert_eq!(error.kind(), RequestFailureKind::Offline);
            assert!(error.message().contains("timed out"), "{}", error.message());
        }
    }

    #[tokio::test]
    async fn oversized_documents_are_rejected_before_the_body_is_buffered() {
        use faf_domain::state::RequestFailureKind;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.api+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        MAX_DOCUMENT_BYTES + 1
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response headers");
        });

        let error = fetch_document_typed(
            &reqwest::Client::new(),
            url::Url::parse(&format!("http://{address}/data/test")).expect("test URL"),
            "token",
        )
        .await
        .expect_err("oversized document must fail");

        assert_eq!(error.kind(), RequestFailureKind::Unexpected);
        assert!(error.message().contains("too large"));
        server.await.expect("test server task");
    }
}
