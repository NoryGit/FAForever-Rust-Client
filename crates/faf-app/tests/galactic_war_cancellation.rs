use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use faf_app::infra::fake_ports;
use faf_app::ports::{GalacticWarPort, InstallProgress};
use faf_app::{App, Ports};
use faf_domain::state::{
    ClientVersions, GalacticWarCommand, GalacticWarStatistics, GalacticWarStatus,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct HeldInstall {
    sender: Mutex<Option<mpsc::Sender<InstallProgress>>>,
    cancel: Mutex<Option<CancellationToken>>,
    launches: AtomicUsize,
}

#[async_trait]
impl GalacticWarPort for HeldInstall {
    async fn statistics(&self) -> Result<GalacticWarStatistics, String> {
        Ok(GalacticWarStatistics::default())
    }
    async fn versions(&self) -> Result<ClientVersions, String> {
        Ok(ClientVersions {
            required_version: "1.0".into(),
            latest_version: Some("2.0".into()),
        })
    }
    fn installed_version(&self) -> Option<String> {
        None
    }
    async fn install(&self, _version: String) -> mpsc::Receiver<InstallProgress> {
        unreachable!("installation must pass cancellation")
    }
    async fn install_cancellable(
        &self,
        _version: String,
        cancel: CancellationToken,
    ) -> mpsc::Receiver<InstallProgress> {
        let (tx, rx) = mpsc::channel(4);
        *self.sender.lock().unwrap() = Some(tx);
        *self.cancel.lock().unwrap() = Some(cancel);
        rx
    }
    async fn launch(&self) -> Result<(), String> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn is_running(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn cancelling_galactic_war_never_auto_launches_even_if_install_just_committed() {
    for outcome in [Err("installation cancelled".into()), Ok("2.0".into())] {
        let installer = Arc::new(HeldInstall::default());
        let (app, app_loop) = App::new(
            "test",
            Ports {
                galactic_war: installer.clone(),
                ..fake_ports()
            },
        );
        tokio::spawn(app_loop.run());
        app.dispatch_and_wait(GalacticWarCommand::Refresh.into())
            .await
            .unwrap();
        let app = Arc::new(app);
        let play = {
            let app = app.clone();
            tokio::spawn(async move {
                app.dispatch_and_wait(GalacticWarCommand::Play.into())
                    .await
                    .unwrap();
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while installer.cancel.lock().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        app.dispatch_and_wait(GalacticWarCommand::CancelInstall.into())
            .await
            .unwrap();
        assert!(installer
            .cancel
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled());
        let sender = installer.sender.lock().unwrap().take().unwrap();
        sender
            .send(InstallProgress::Finished(outcome.clone()))
            .await
            .unwrap();
        drop(sender);
        tokio::time::timeout(std::time::Duration::from_secs(2), play)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(installer.launches.load(Ordering::SeqCst), 0);
        assert_eq!(app.snapshot().galactic_war.status, GalacticWarStatus::Idle);
        assert_eq!(app.snapshot().galactic_war.installed_version, outcome.ok());
    }
}
