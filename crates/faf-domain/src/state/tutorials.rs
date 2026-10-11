//! Tutorials: FAF's guided single-player lessons.
//!
//! The Java client is the reference (`tutorial/TutorialService`): categories
//! come from `/data/tutorialCategory` with their tutorials nested, and playing
//! one launches an *offline* game on the tutorial's map with the `tutorials`
//! featured mod.
//!
//! The Python client's `tutorials/` is not a usable reference. Its list
//! arrives over the lobby socket as `tutorialsInfo` pushes: a legacy
//! server-side feature: and "playing" a tutorial there downloads a
//! `.fafreplay` and watches it, which is a different thing entirely.

use serde::{Deserialize, Serialize};
use specta::Type;

/// The featured mod a tutorial runs under. Java's
/// `KnownFeaturedMod.TUTORIALS`.
pub const TUTORIALS_FEATURED_MOD: &str = "tutorials";

/// One lesson.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct Tutorial {
    pub id: i32,
    pub title: String,
    /// Already reduced to plain text; the API stores it as HTML.
    pub description: String,
    /// The first `https://` destination the description linked to, or empty.
    ///
    /// FAF publishes whole categories: "Video tutorials", "Written guides",
    /// whose entries are not playable maps at all but pointers to YouTube and
    /// the wiki. Reducing the description to plain text keeps their prose and
    /// throws away the only thing they are for, which left most of the tab
    /// showing rows labelled "unavailable" that did nothing when clicked.
    pub link_url: String,
    pub image_url: String,
    /// Position within its category, as the API orders them.
    pub ordinal: i32,
    /// Whether the server considers this one playable. A tutorial can be
    /// listed for reference while its map is being replaced.
    pub launchable: bool,
    /// The map the lesson runs on: needed before launching.
    pub map_folder_name: String,
    /// The scenario name passed to the game as `/map`.
    pub technical_name: String,
    pub category_id: Option<i32>,
}

impl Tutorial {
    /// Whether this can actually be started.
    ///
    /// Stricter than the server's `launchable` flag alone: without a map
    /// folder there is nothing to download, and without a technical name there
    /// is no `/map` argument, so the game would open to the main menu and look
    /// like the client did nothing.
    pub fn is_playable(&self) -> bool {
        self.launchable && !self.map_folder_name.is_empty() && !self.technical_name.is_empty()
    }

    /// Whether this entry is a pointer to external material rather than a map.
    ///
    /// Checked after [`Self::is_playable`]: a playable lesson whose description
    /// happens to cite a video is still a lesson, not a link.
    pub fn is_link(&self) -> bool {
        !self.is_playable() && !self.link_url.is_empty()
    }
}

/// A group of lessons: "Basics", "Build order", and so on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct TutorialCategory {
    pub id: i32,
    pub name: String,
}

/// Where a launch stands. Its own status because the wait is long: the
/// `tutorials` featured mod has to be patched and the map downloaded first.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum TutorialLaunchStatus {
    #[default]
    Idle,
    #[serde(rename_all = "camelCase")]
    Preparing { tutorial_id: i32, detail: String },
    #[serde(rename_all = "camelCase")]
    Launched { tutorial_id: i32 },
    /// Keyed by the lesson like the other two, so one lesson's failure is not
    /// shown under every lesson the player opens afterwards.
    #[serde(rename_all = "camelCase")]
    Failed { tutorial_id: i32, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum TutorialsStatus {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct TutorialsState {
    pub categories: Vec<TutorialCategory>,
    pub tutorials: Vec<Tutorial>,
    pub status: TutorialsStatus,
    pub selected_id: Option<i32>,
    pub launch: TutorialLaunchStatus,
}

impl TutorialsState {
    pub fn selected(&self) -> Option<&Tutorial> {
        let id = self.selected_id?;
        self.tutorials.iter().find(|tutorial| tutorial.id == id)
    }
}

/// The lessons in one category, in teaching order.
pub fn tutorials_of(tutorials: &[Tutorial], category_id: i32) -> Vec<&Tutorial> {
    let mut found: Vec<&Tutorial> = tutorials
        .iter()
        .filter(|tutorial| tutorial.category_id == Some(category_id))
        .collect();
    // `ordinal` is the author's teaching order and is the whole point of a
    // tutorial list; title is only a tie-break for records that share one.
    found.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.title.cmp(&right.title))
    });
    found
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum TutorialsEvent {
    LaunchCancelled,
    Loading,
    Loaded {
        categories: Vec<TutorialCategory>,
        tutorials: Vec<Tutorial>,
    },
    LoadFailed {
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    Selected {
        tutorial_id: i32,
    },
    #[serde(rename_all = "camelCase")]
    LaunchPreparing {
        tutorial_id: i32,
        detail: String,
    },
    #[serde(rename_all = "camelCase")]
    Launched {
        tutorial_id: i32,
    },
    #[serde(rename_all = "camelCase")]
    LaunchFailed {
        tutorial_id: i32,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum TutorialsCommand {
    CancelLaunch,
    Load,
    #[serde(rename_all = "camelCase")]
    Select {
        tutorial_id: i32,
    },
    #[serde(rename_all = "camelCase")]
    Launch {
        tutorial_id: i32,
    },
}

pub fn reduce(state: &mut TutorialsState, event: &TutorialsEvent) {
    match event {
        TutorialsEvent::LaunchCancelled => state.launch = TutorialLaunchStatus::Idle,
        TutorialsEvent::Loading => state.status = TutorialsStatus::Loading,
        TutorialsEvent::Loaded {
            categories,
            tutorials,
        } => {
            state.categories = categories.clone();
            state.tutorials = tutorials.clone();
            state.status = TutorialsStatus::Ready;
            let still_present = state
                .selected_id
                .is_some_and(|id| tutorials.iter().any(|tutorial| tutorial.id == id));
            if !still_present {
                state.selected_id = tutorials.first().map(|tutorial| tutorial.id);
            }
        }
        TutorialsEvent::LoadFailed { reason } => {
            state.status = TutorialsStatus::Failed {
                reason: reason.clone(),
            }
        }
        TutorialsEvent::Selected { tutorial_id } => state.selected_id = Some(*tutorial_id),
        TutorialsEvent::LaunchPreparing {
            tutorial_id,
            detail,
        } => {
            state.launch = TutorialLaunchStatus::Preparing {
                tutorial_id: *tutorial_id,
                detail: detail.clone(),
            }
        }
        TutorialsEvent::Launched { tutorial_id } => {
            state.launch = TutorialLaunchStatus::Launched {
                tutorial_id: *tutorial_id,
            }
        }
        TutorialsEvent::LaunchFailed {
            tutorial_id,
            reason,
        } => {
            state.launch = TutorialLaunchStatus::Failed {
                tutorial_id: *tutorial_id,
                reason: reason.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tutorial(id: i32, ordinal: i32, title: &str) -> Tutorial {
        Tutorial {
            id,
            title: title.into(),
            description: String::new(),
            link_url: String::new(),
            image_url: String::new(),
            ordinal,
            launchable: true,
            map_folder_name: format!("scmp_tut_{id}"),
            technical_name: format!("tut_{id}"),
            category_id: Some(1),
        }
    }

    #[test]
    fn a_guide_entry_is_a_link_rather_than_an_unavailable_lesson() {
        // FAF's "Video tutorials" and "Written guides" categories are not maps.
        // Before `link_url` they showed as rows labelled "unavailable" whose
        // only content: the destination: had been stripped away.
        let mut guide = tutorial(9, 1, "Video Tutorials from Heaven on Youtube");
        guide.launchable = false;
        guide.map_folder_name = String::new();
        guide.technical_name = String::new();
        guide.link_url = "https://www.youtube.com/watch?v=abc".into();

        assert!(!guide.is_playable());
        assert!(guide.is_link());
    }

    #[test]
    fn a_playable_lesson_is_never_reduced_to_a_link() {
        // A lesson whose briefing happens to cite a video is still a lesson.
        let mut lesson = tutorial(3, 1, "Four-Leaf Clover");
        lesson.link_url = "https://www.youtube.com/watch?v=abc".into();
        assert!(lesson.is_playable());
        assert!(!lesson.is_link());
    }

    #[test]
    fn an_entry_with_neither_a_map_nor_a_link_is_simply_unavailable() {
        let mut stub = tutorial(4, 1, "Coming soon");
        stub.launchable = false;
        stub.map_folder_name = String::new();
        stub.technical_name = String::new();
        assert!(!stub.is_playable());
        assert!(!stub.is_link());
    }

    #[test]
    fn lessons_are_listed_in_teaching_order_not_alphabetically() {
        // The ordinal is the author's curriculum. Sorting by title would put
        // "Advanced eco" before "Basics".
        let tutorials = vec![
            tutorial(3, 3, "Advanced eco"),
            tutorial(1, 1, "Zeroth lesson"),
            tutorial(2, 2, "Building"),
        ];
        let found = tutorials_of(&tutorials, 1);
        assert_eq!(
            found.iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn lessons_from_another_category_are_excluded() {
        let tutorials = vec![
            tutorial(1, 1, "Ours"),
            Tutorial {
                category_id: Some(2),
                ..tutorial(2, 1, "Theirs")
            },
            Tutorial {
                category_id: None,
                ..tutorial(3, 1, "Orphan")
            },
        ];
        assert_eq!(tutorials_of(&tutorials, 1).len(), 1);
    }

    #[test]
    fn a_tutorial_needs_more_than_the_launchable_flag_to_be_playable() {
        // Launching without a map opens the game to its main menu, which looks
        // exactly like the client having done nothing.
        assert!(tutorial(1, 1, "Fine").is_playable());

        assert!(!Tutorial {
            launchable: false,
            ..tutorial(1, 1, "Withdrawn")
        }
        .is_playable());

        assert!(!Tutorial {
            map_folder_name: String::new(),
            ..tutorial(1, 1, "No map")
        }
        .is_playable());

        assert!(!Tutorial {
            technical_name: String::new(),
            ..tutorial(1, 1, "No scenario")
        }
        .is_playable());
    }

    #[test]
    fn the_first_lesson_opens_on_the_first_load() {
        let mut state = TutorialsState::default();
        reduce(&mut state, &TutorialsEvent::Loading);
        assert_eq!(state.status, TutorialsStatus::Loading);

        reduce(
            &mut state,
            &TutorialsEvent::Loaded {
                categories: vec![TutorialCategory {
                    id: 1,
                    name: "Basics".into(),
                }],
                tutorials: vec![tutorial(7, 1, "A"), tutorial(8, 2, "B")],
            },
        );
        assert_eq!(state.status, TutorialsStatus::Ready);
        assert_eq!(state.selected_id, Some(7));
    }

    #[test]
    fn refreshing_keeps_the_open_lesson_but_drops_one_that_vanished() {
        let mut state = TutorialsState {
            selected_id: Some(8),
            ..TutorialsState::default()
        };
        reduce(
            &mut state,
            &TutorialsEvent::Loaded {
                categories: Vec::new(),
                tutorials: vec![tutorial(7, 1, "A"), tutorial(8, 2, "B")],
            },
        );
        assert_eq!(state.selected_id, Some(8));

        reduce(
            &mut state,
            &TutorialsEvent::Loaded {
                categories: Vec::new(),
                tutorials: vec![tutorial(7, 1, "A")],
            },
        );
        assert_eq!(state.selected_id, Some(7));
    }

    #[test]
    fn a_failed_load_keeps_whatever_was_already_listed() {
        let mut state = TutorialsState {
            tutorials: vec![tutorial(7, 1, "A")],
            selected_id: Some(7),
            status: TutorialsStatus::Ready,
            ..TutorialsState::default()
        };
        reduce(
            &mut state,
            &TutorialsEvent::LoadFailed {
                reason: "503".into(),
            },
        );
        assert_eq!(state.tutorials.len(), 1);
        assert_eq!(state.selected_id, Some(7));
    }

    #[test]
    fn a_launch_narrates_its_wait_then_settles() {
        // Patching the tutorials mod and fetching the map takes long enough
        // that a silent client looks broken.
        let mut state = TutorialsState::default();
        reduce(
            &mut state,
            &TutorialsEvent::LaunchPreparing {
                tutorial_id: 7,
                detail: "Updating tutorials".into(),
            },
        );
        assert_eq!(
            state.launch,
            TutorialLaunchStatus::Preparing {
                tutorial_id: 7,
                detail: "Updating tutorials".into()
            }
        );

        reduce(&mut state, &TutorialsEvent::Launched { tutorial_id: 7 });
        assert_eq!(
            state.launch,
            TutorialLaunchStatus::Launched { tutorial_id: 7 }
        );
    }

    #[test]
    fn a_failed_launch_reports_why_and_for_which_lesson() {
        // Keyed, so the view can show it under the lesson that failed and not
        // under every lesson opened after it.
        let mut state = TutorialsState::default();
        reduce(
            &mut state,
            &TutorialsEvent::LaunchFailed {
                tutorial_id: 7,
                reason: "no install".into(),
            },
        );
        assert_eq!(
            state.launch,
            TutorialLaunchStatus::Failed {
                tutorial_id: 7,
                reason: "no install".into()
            }
        );
    }

    #[test]
    fn the_selected_lesson_is_resolvable() {
        let state = TutorialsState {
            tutorials: vec![tutorial(7, 1, "A"), tutorial(8, 2, "B")],
            selected_id: Some(8),
            ..TutorialsState::default()
        };
        assert_eq!(state.selected().map(|t| t.id), Some(8));
    }
}
