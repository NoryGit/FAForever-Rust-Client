//! The map catalogue is crawled once, not once per tab visit.
//!
//! `LoadVault` is the most expensive command this client has: it walks the whole
//! FAF map catalogue. Most callers checked `vaultStatus` before sending it, but
//! the two on the Play tab did not, so every visit to Play threw a finished
//! catalogue away and crawled it again. The guard now lives in the service, and
//! this pins it there.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use faf_app::infra::fake_ports;
use faf_app::ports::{MapSearchPage, MapsPort};
use faf_app::{App, Ports};
use faf_domain::protocol::vault_query::MapVaultQuery;
use faf_domain::state::{InstalledMap, MapListStatus, MapsCommand, MatchmakerMapPool, VaultMap};
use tokio::sync::mpsc;

/// Counts crawls. `list_vault` is the only method under test.
#[derive(Default)]
struct CountingMaps {
    crawls: Arc<AtomicUsize>,
    fail: bool,
}

#[async_trait]
impl MapsPort for CountingMaps {
    async fn list_vault_with_progress(
        &self,
        progress: Option<mpsc::Sender<faf_domain::state::maps::CatalogueProgress>>,
    ) -> Result<Vec<VaultMap>, String> {
        if let Some(progress) = progress {
            let _ = progress
                .send(faf_domain::state::maps::CatalogueProgress {
                    pages: 2,
                    total_pages: Some(5),
                })
                .await;
        }
        self.list_vault().await
    }
    async fn list_vault(&self) -> Result<Vec<VaultMap>, String> {
        self.crawls.fetch_add(1, Ordering::SeqCst);
        // Long enough for callers asking together to overlap, as views
        // mounting together do against the real API.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if self.fail {
            return Err("the vault is unreachable".into());
        }
        Ok(Vec::new())
    }

    async fn search_vault(&self, _query: MapVaultQuery) -> Result<MapSearchPage, String> {
        unreachable!("this test only drives the catalogue crawl")
    }

    async fn list_installed(&self) -> Result<Vec<InstalledMap>, String> {
        Ok(Vec::new())
    }

    async fn list_matchmaker_pools(
        &self,
        _queue_name: String,
    ) -> Result<Vec<MatchmakerMapPool>, String> {
        Ok(Vec::new())
    }

    async fn install_map(
        &self,
        _folder_name: String,
        _download_url: String,
    ) -> Result<Vec<InstalledMap>, String> {
        unreachable!()
    }

    async fn uninstall_map(&self, _folder_name: String) -> Result<Vec<InstalledMap>, String> {
        unreachable!()
    }

    async fn set_map_version_hidden(&self, _version_id: i32, _hidden: bool) -> Result<(), String> {
        unreachable!("this test only drives the catalogue crawl")
    }
}

#[tokio::test]
async fn cancelling_a_catalogue_drops_the_read_and_allows_a_retry() {
    let (app, crawls) = app_with(false);
    let app = Arc::new(app);
    let load = {
        let app = app.clone();
        tokio::spawn(async move {
            app.dispatch_and_wait(MapsCommand::LoadVault.into())
                .await
                .unwrap();
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while app.snapshot().maps.vault_progress.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(app.snapshot().maps.vault_progress.unwrap().pages, 2);
    app.dispatch_and_wait(MapsCommand::CancelVaultLoad.into())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), load)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(70)).await;
    assert_eq!(app.snapshot().maps.vault_status, MapListStatus::Cancelled);
    assert!(app.snapshot().maps.vault_progress.is_none());
    app.dispatch_and_wait(MapsCommand::LoadVault.into())
        .await
        .unwrap();
    assert_eq!(app.snapshot().maps.vault_status, MapListStatus::Ready);
    assert_eq!(crawls.load(Ordering::SeqCst), 2);
}

fn app_with(fail: bool) -> (App, Arc<AtomicUsize>) {
    let crawls = Arc::new(AtomicUsize::new(0));
    let ports = Ports {
        maps: Arc::new(CountingMaps {
            crawls: crawls.clone(),
            fail,
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    (app, crawls)
}

#[tokio::test]
async fn a_loaded_catalogue_is_not_crawled_again() {
    let (app, crawls) = app_with(false);

    app.dispatch_and_wait(MapsCommand::LoadVault.into())
        .await
        .unwrap();
    assert_eq!(app.snapshot().maps.vault_status, MapListStatus::Ready);

    // Opening the Play tab, then the host dialog, then Play again.
    for _ in 0..3 {
        app.dispatch_and_wait(MapsCommand::LoadVault.into())
            .await
            .unwrap();
    }

    assert_eq!(
        crawls.load(Ordering::SeqCst),
        1,
        "the catalogue must be crawled once, however many callers ask for it"
    );
    assert_eq!(app.snapshot().maps.vault_status, MapListStatus::Ready);
}

/// The guard is about repeating *successful* work. A vault that failed, because
/// the user was offline for the first attempt, has to be retryable or the tab
/// stays empty for the rest of the session.
#[tokio::test]
async fn a_failed_catalogue_is_retried() {
    let (app, crawls) = app_with(true);

    app.dispatch_and_wait(MapsCommand::LoadVault.into())
        .await
        .unwrap();
    assert!(matches!(
        app.snapshot().maps.vault_status,
        MapListStatus::Failed { .. }
    ));

    app.dispatch_and_wait(MapsCommand::LoadVault.into())
        .await
        .unwrap();

    assert_eq!(
        crawls.load(Ordering::SeqCst),
        2,
        "a failure must be retryable"
    );
}

/// Several views mounting together each ask for the catalogue. The `Loading`
/// status alone did not stop that: commands run concurrently, so callers asking
/// at once could all read the status before any crawl had set it, and each
/// crawled every page.
#[tokio::test]
async fn callers_asking_at_once_share_one_crawl() {
    let (app, crawls) = app_with(false);

    let asks = (0..5).map(|_| app.dispatch_and_wait(MapsCommand::LoadVault.into()));
    for result in futures_util::future::join_all(asks).await {
        result.unwrap();
    }

    assert_eq!(
        crawls.load(Ordering::SeqCst),
        1,
        "callers asking together must share one crawl"
    );
    assert_eq!(app.snapshot().maps.vault_status, MapListStatus::Ready);
}
