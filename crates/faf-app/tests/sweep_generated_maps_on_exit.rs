//! The sweep of generated maps as the client exits (#720).
//!
//! The client only ever deleted generated maps from the Maps tab's clean-up
//! button, so with "Keep generated maps" off they still stayed on disk for
//! good. The shell now sweeps them on exit, as the Java client does; these pin
//! what that sweep spares and when it leaves everything alone.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use faf_app::infra::{fake_ports, FakeMapGenerator};
use faf_app::ports::{GeneratorUpdate, MapGeneratorPort, SettingsPort};
use faf_app::App;
use faf_domain::state::{GeneratorOptionQuery, GeneratorOptions, GeneratorPreset, SettingsState};
use tokio::sync::mpsc;

/// The offline generator, recording what each clean-up was told to spare.
#[derive(Default)]
struct RecordingGenerator {
    sweeps: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl MapGeneratorPort for RecordingGenerator {
    async fn generate_named(&self, map_name: String) -> mpsc::Receiver<GeneratorUpdate> {
        FakeMapGenerator.generate_named(map_name).await
    }
    async fn generate(&self, options: GeneratorOptions) -> mpsc::Receiver<GeneratorUpdate> {
        FakeMapGenerator.generate(options).await
    }
    async fn query_options(
        &self,
        query: GeneratorOptionQuery,
        version: Option<String>,
        progress: Option<mpsc::Sender<GeneratorUpdate>>,
    ) -> Result<Vec<String>, String> {
        FakeMapGenerator
            .query_options(query, version, progress)
            .await
    }
    async fn preflight(&self, options: GeneratorOptions) -> Result<String, String> {
        FakeMapGenerator.preflight(options).await
    }
    async fn help(&self, version: Option<String>) -> Result<String, String> {
        FakeMapGenerator.help(version).await
    }
    fn cancel(&self) {}
    async fn save_preset(&self, name: &str, options: &GeneratorOptions) -> Result<(), String> {
        FakeMapGenerator.save_preset(name, options).await
    }
    async fn list_presets(&self) -> Vec<GeneratorPreset> {
        FakeMapGenerator.list_presets().await
    }
    async fn rename_preset(&self, name: &str, new_name: &str) -> Result<(), String> {
        FakeMapGenerator.rename_preset(name, new_name).await
    }
    async fn delete_preset(&self, name: &str) -> Result<(), String> {
        FakeMapGenerator.delete_preset(name).await
    }
    async fn latest_version(&self) -> Result<String, String> {
        FakeMapGenerator.latest_version().await
    }
    async fn available_versions(&self) -> Result<Vec<String>, String> {
        FakeMapGenerator.available_versions().await
    }
    fn is_installed(&self, map_name: &str) -> bool {
        FakeMapGenerator.is_installed(map_name)
    }
    async fn clean_up(&self, protected_maps: &[String]) -> Result<usize, String> {
        self.sweeps.lock().unwrap().push(protected_maps.to_vec());
        Ok(2)
    }
    async fn map_previews(
        &self,
        map_names: &[String],
    ) -> std::collections::HashMap<String, String> {
        FakeMapGenerator.map_previews(map_names).await
    }
}

/// A settings file that cannot be read, as on a damaged or locked disk.
struct UnreadableSettings;

#[async_trait]
impl SettingsPort for UnreadableSettings {
    async fn load(&self) -> Option<SettingsState> {
        None
    }
    async fn save(&self, _settings: &SettingsState) -> Result<(), String> {
        Ok(())
    }
}

fn stored_settings() -> SettingsState {
    let mut settings = SettingsState::default();
    settings.browsing.favorite_maps = vec!["neroxis_map_generator_1.7.7_fav".into()];
    settings.kept_generated_maps = vec!["neroxis_map_generator_1.7.7_kept".into()];
    settings
}

#[tokio::test]
async fn the_exit_sweep_spares_favourites_and_kept_maps() {
    let generator = Arc::new(RecordingGenerator::default());
    let mut ports = fake_ports();
    ports.map_generator = generator.clone();
    ports.settings = Arc::new(faf_app::infra::FakeSettings {
        initial: stored_settings(),
    });
    let (app, _loop) = App::new("test", ports);

    // From the settings file, not the state: this app's loop never ran, so
    // its state never loaded them, which is the case the file is read for.
    assert_eq!(app.sweep_generated_maps().await, Ok(2));
    assert_eq!(
        *generator.sweeps.lock().unwrap(),
        vec![vec![
            "neroxis_map_generator_1.7.7_fav".to_string(),
            "neroxis_map_generator_1.7.7_kept".to_string(),
        ]],
    );
}

#[tokio::test]
async fn an_unreadable_settings_file_deletes_nothing() {
    // Without the file there is no telling which maps are favourites or kept,
    // and sweeping anyway would delete exactly those.
    let generator = Arc::new(RecordingGenerator::default());
    let mut ports = fake_ports();
    ports.map_generator = generator.clone();
    ports.settings = Arc::new(UnreadableSettings);
    let (app, app_loop) = App::new("test", ports);

    assert!(app.sweep_generated_maps().await.is_err());
    let task = tokio::spawn(app_loop.run());
    app.dispatch_and_wait(faf_domain::state::MapGeneratorCommand::CleanUp.into())
        .await
        .unwrap();
    assert!(generator.sweeps.lock().unwrap().is_empty());
    task.abort();
}

#[derive(Default)]
struct SavingSettings {
    started: tokio::sync::Notify,
    finish: tokio::sync::Notify,
}

#[async_trait]
impl SettingsPort for SavingSettings {
    async fn load(&self) -> Option<SettingsState> {
        Some(stored_settings())
    }

    async fn save(&self, _settings: &SettingsState) -> Result<(), String> {
        self.started.notify_one();
        self.finish.notified().await;
        Ok(())
    }
}

#[tokio::test]
async fn both_sweeps_spare_a_new_favourite_while_its_save_is_pending() {
    use faf_domain::state::{settings::PreferenceList, MapGeneratorCommand, SettingsCommand};

    let generator = Arc::new(RecordingGenerator::default());
    let settings = Arc::new(SavingSettings::default());
    let (app, app_loop) = App::new(
        "test",
        faf_app::ports::Ports {
            map_generator: generator.clone(),
            settings: settings.clone(),
            ..fake_ports()
        },
    );
    let task = tokio::spawn(app_loop.run());
    app.dispatch_and_wait(SettingsCommand::Load.into())
        .await
        .unwrap();

    let map = "neroxis_map_generator_1.7.7_just_starred";
    app.dispatch(
        SettingsCommand::SetListMember {
            list: PreferenceList::FavoriteMaps,
            value: map.into(),
            member: true,
        }
        .into(),
    )
    .await
    .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        settings.started.notified(),
    )
    .await
    .expect("the save started");

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        app.sweep_generated_maps().await.unwrap();
        app.dispatch_and_wait(MapGeneratorCommand::CleanUp.into())
            .await
            .unwrap();
    })
    .await
    .expect("both sweeps completed before the save");
    let sweeps = generator.sweeps.lock().unwrap();
    assert_eq!(sweeps.len(), 2);
    for protected in sweeps.iter() {
        for expected in [
            "neroxis_map_generator_1.7.7_fav",
            "neroxis_map_generator_1.7.7_kept",
            map,
        ] {
            assert!(
                protected.iter().any(|name| name == expected),
                "unprotected map: {expected}"
            );
        }
    }
    settings.finish.notify_one();
    task.abort();
}
