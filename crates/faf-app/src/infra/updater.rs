//! Real game updater: patches the featured mod and stages the map for a
//! live game.
//!
//! The adapter half of [`GameUpdaterPort`]; the mechanics (file lists, MD5
//! diffing, the content-addressed cache, the executable version stamp, the
//! vault CDN) all live in [`crate::infra::game_updater`], shared with replay
//! playback. What is specific here is the *live* context:
//!
//! - the install being patched is the one Settings currently points live games
//!   at, asked for through [`ProcessPort::game_install_dir`] on every run
//!   rather than captured at startup;
//! - the version is `latest`, because a live game has none to read;
//! - the map lands in the user's maps folder, where a live game looks for it.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::infra::session::TokenStore;
use crate::infra::{cache_dir, env_or, game_updater, maps};
use crate::ports::{
    GamePreparation, GameUpdaterPort, InstalledBuild, PreparationStep, ProcessPort, UpdateProgress,
};

/// Endpoints and install details the updater needs.
#[derive(Debug, Clone)]
pub struct UpdaterConfig {
    /// FAF Data API root: featured mods and their file lists.
    pub api_base: String,
    /// Public content CDN: `GET {content_base}/maps/{folder}.zip`.
    pub content_base: String,
    /// The engine executable's filename inside the install's `bin/`.
    pub exe_name: String,
}

impl UpdaterConfig {
    pub fn faf() -> Self {
        Self {
            api_base: env_or("FAF_API_BASE", "https://api.faforever.com"),
            content_base: env_or("FAF_CONTENT_BASE", "https://content.faforever.com"),
            exe_name: env_or("FAF_GAME_EXE_NAME", "ForgedAlliance.exe"),
        }
    }
}

pub struct GameUpdaterClient {
    config: UpdaterConfig,
    tokens: TokenStore,
    http: reqwest::Client,
    /// Consulted for the live install directory, so a path changed in Settings
    /// takes effect on the very next launch.
    process: Arc<dyn ProcessPort>,
}

impl GameUpdaterClient {
    pub fn new(config: UpdaterConfig, tokens: TokenStore, process: Arc<dyn ProcessPort>) -> Self {
        Self {
            config,
            tokens,
            http: super::http::shared_http_client(),
            process,
        }
    }

    pub fn faf(tokens: TokenStore, process: Arc<dyn ProcessPort>) -> Self {
        Self::new(UpdaterConfig::faf(), tokens, process)
    }

    /// The whole run, as one fallible unit. `progress` is called with each
    /// user-facing step.
    async fn run(
        &self,
        request: &GamePreparation,
        progress: &(dyn Fn(PreparationStep) + Sync),
    ) -> Result<(), String> {
        let target_dir = self.process.game_install_dir().ok_or_else(|| {
            "no Forged Alliance install is configured: set one in Settings → Paths".to_string()
        })?;
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        game_updater::ensure_latest_game_version(
            &self.http,
            &token,
            &self.config.api_base,
            &cache_dir()?.join("game_files"),
            &target_dir,
            &request.featured_mod,
            &self.config.exe_name,
            request.cache_rolling_branches,
            progress,
        )
        .await
        .map_err(|e| format!("could not update {}: {e}", request.featured_mod))?;

        // Unlike the replay path, a failure here is fatal. There the map is a
        // best-effort convenience for a recording that may not even need one;
        // here the server has already seated the player in a game on this map,
        // so launching without it just trades a clear message for a hang on a
        // blank loading screen.
        if let Some(folder) = &request.map_folder {
            game_updater::ensure_live_map(
                &self.http,
                &self.config.content_base,
                &maps::maps_dir(),
                folder,
                progress,
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl GameUpdaterPort for GameUpdaterClient {
    async fn prepare(&self, request: GamePreparation) -> mpsc::Receiver<UpdateProgress> {
        self.prepare_cancellable(request, tokio_util::sync::CancellationToken::new())
            .await
    }

    async fn prepare_cancellable(
        &self,
        request: GamePreparation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> mpsc::Receiver<UpdateProgress> {
        // Bounded, because progress is a status line: if the consumer falls
        // behind, dropping intermediate steps is correct and losing the
        // terminal `Finished` is not: hence `send().await` below, which
        // applies backpressure rather than discarding.
        let (tx, rx) = mpsc::channel(32);

        let config = self.config.clone();
        let tokens = self.tokens.clone();
        let http = self.http.clone();
        let process = self.process.clone();
        tokio::spawn(async move {
            let client = GameUpdaterClient {
                config,
                tokens,
                http,
                process,
            };
            // The synchronous callback can't await, so steps go through a
            // second channel that the loop below drains alongside the run.
            let (steps_tx, mut steps_rx) = mpsc::unbounded_channel::<PreparationStep>();
            let report = move |step: PreparationStep| {
                let _ = steps_tx.send(step);
            };

            let forward = tx.clone();
            let pump = tokio::spawn(async move {
                while let Some(step) = steps_rx.recv().await {
                    if forward.send(UpdateProgress::Step(step)).await.is_err() {
                        break;
                    }
                }
            });

            let outcome = tokio::select! {
                biased;
                () = cancel.cancelled() => Err("preparation cancelled".to_string()),
                result = client.run(&request, &report) => result,
            };
            drop(report); // closes `steps_rx`, ending the pump
            let _ = pump.await;
            let _ = tx.send(UpdateProgress::Finished(outcome)).await;
        });

        rx
    }

    async fn ensure_maps(&self, folders: &[String]) -> Vec<(String, String)> {
        let mut failures = Vec::new();
        let maps_dir = maps::maps_dir();
        for folder in folders {
            // Already on disk is the common case, and costs a directory read.
            if let Err(reason) = game_updater::ensure_live_map(
                &self.http,
                &self.config.content_base,
                &maps_dir,
                folder,
                &|_| {},
            )
            .await
            {
                failures.push((folder.clone(), reason));
            }
        }
        failures
    }

    fn installed_build(&self) -> Option<InstalledBuild> {
        // Asked for on every launch, like `run` above, so the stamp read is
        // always the one in the install Settings currently points at.
        game_updater::read_installed_build(&self.process.game_install_dir()?)
    }
}

/// Inert updater: used offline and in tests. Reports success without touching
/// anything, matching [`crate::infra::FakeGame`]'s posture: there is no install
/// to patch, and failing would block a launch path that is itself a no-op.
#[derive(Debug, Clone, Default)]
pub struct FakeGameUpdater;

#[async_trait]
impl GameUpdaterPort for FakeGameUpdater {
    async fn prepare(&self, _request: GamePreparation) -> mpsc::Receiver<UpdateProgress> {
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(UpdateProgress::Finished(Ok(()))).await;
        rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_fake_always_finishes_successfully_and_closes() {
        let mut rx = FakeGameUpdater
            .prepare(GamePreparation {
                featured_mod: "faf".into(),
                map_folder: Some("adaptive_gadostb.v0002".into()),
                cache_rolling_branches: false,
            })
            .await;
        assert_eq!(rx.recv().await, Some(UpdateProgress::Finished(Ok(()))));
        assert_eq!(rx.recv().await, None, "the stream must end after Finished");
    }

    #[tokio::test]
    async fn without_a_configured_install_the_run_fails_before_any_network_call() {
        // `FakeGame` reports no install dir, and the client is given no token,
        // if the order were reversed this would fail with "not logged in".
        let client = GameUpdaterClient::new(
            UpdaterConfig {
                api_base: "http://127.0.0.1:1/api".into(),
                content_base: "http://127.0.0.1:1".into(),
                exe_name: "ForgedAlliance.exe".into(),
            },
            TokenStore::new(),
            Arc::new(crate::infra::FakeGame),
        );

        let mut rx = client
            .prepare(GamePreparation {
                featured_mod: "faf".into(),
                map_folder: None,
                cache_rolling_branches: false,
            })
            .await;

        let mut last = None;
        while let Some(update) = rx.recv().await {
            last = Some(update);
        }
        let Some(UpdateProgress::Finished(Err(reason))) = last else {
            panic!("expected a failure, got {last:?}");
        };
        assert!(
            reason.contains("Settings"),
            "the message should point at the fix, got: {reason}"
        );
    }
}
