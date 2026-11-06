//! Social service.
//!
//! Owns the friend/foe commands. The transport is the lobby socket (that is
//! where `social_add`/`social_remove` live), but the state they change belongs
//! to the social slice, so the command lives here rather than in
//! [`services::lobby`](crate::services::lobby): same split as the slices.
//!
//! The server does not acknowledge individual writes. Publish the optimistic
//! change only after the lobby accepts all its messages. The next server
//! snapshot reconciles the list if the connection drops after admission.
//!
//! The connection keeps a second copy of the same fact, because it re-sends
//! the relation lists whenever a `player_info` names an account they were
//! waiting on: see `PlayerDirectory::note_local_relation`. Without it that
//! re-send undid whatever the user had just done.

use faf_domain::state::{Contact, NotificationKind, Relation, SocialCommand, SocialEvent};

use crate::runtime::{EventSink, ServiceCtx};

pub async fn handle(cmd: SocialCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        SocialCommand::SetContactsOpen { open } => out.emit(SocialEvent::ContactsOpened { open }),
        SocialCommand::ResolveContacts => {
            let ids = out.with_state(|state| {
                state
                    .social
                    .contacts
                    .entries
                    .iter()
                    .filter(|contact| contact.login.is_none())
                    .map(|contact| contact.player_id)
                    .collect::<Vec<_>>()
            });
            if ids.is_empty() {
                return;
            }
            out.emit(SocialEvent::ContactsRefreshing { refreshing: true });
            if let Err(error) = ctx.ports.lobby.resolve_contacts(&ids).await {
                tracing::warn!(%error, "could not resolve contact names");
            }
            out.emit(SocialEvent::ContactsRefreshing { refreshing: false });
        }
        SocialCommand::SetRelation {
            player_id,
            login,
            relation,
            member,
        } => {
            let login = out
                .with_state(|state| {
                    state
                        .social
                        .contacts
                        .entries
                        .iter()
                        .find(|contact| contact.player_id == player_id)
                        .and_then(|contact| contact.login.clone())
                })
                .unwrap_or(login);
            // The slice treats friend and foe as mutually exclusive, and the
            // server does not: `social_add` writes a row per list, so marking
            // a friend as a foe left the account on both and the next login
            // read them back as both. The reducer drops the opposite entry, so
            // the opposite row is dropped on the server too.
            let opposite = match relation {
                Relation::Friend => Relation::Foe,
                Relation::Foe => Relation::Friend,
            };
            let was_opposite = member
                && out.with_state(|state| {
                    let list = match opposite {
                        Relation::Friend => &state.social.friends,
                        Relation::Foe => &state.social.foes,
                    };
                    list.iter().any(|known| known.eq_ignore_ascii_case(&login))
                        || state.social.contacts.entries.iter().any(|contact| {
                            contact.player_id == player_id && contact.relation == opposite
                        })
                });

            let contact = Contact {
                player_id,
                login: (!login.is_empty()).then_some(login),
                relation,
            };
            if !ctx.ports.lobby.set_contact_relation(
                contact.clone(),
                member,
                was_opposite.then_some(opposite),
            ) {
                super::notifications::add_required_text(
                    out,
                    NotificationKind::Error,
                    super::notifications::Text::new("notifications.msg.contactChangeFailed"),
                    "Could not update contacts",
                    "Check the lobby connection and try again.",
                    None,
                );
                return;
            }
            out.emit(SocialEvent::ContactSet { contact, member });
        }
        SocialCommand::LookUpLogin { login } => {
            let login = login.trim().to_string();
            if login.is_empty() {
                return;
            }
            // Online, or answered already: the menu has its id, or knows there
            // is none. Asked again only after a failure, which stores nothing.
            let known = out.with_state(|state| {
                state.social.player(&login).is_some()
                    || state
                        .social
                        .login_lookups
                        .iter()
                        .any(|lookup| lookup.login.eq_ignore_ascii_case(&login))
            });
            if known {
                return;
            }
            match ctx
                .ports
                .player_card
                .players_by_login(std::slice::from_ref(&login))
                .await
            {
                Ok(players) => {
                    let found = players
                        .into_iter()
                        .find(|player| player.login.eq_ignore_ascii_case(&login));
                    // Stored under the account's own spelling when there is
                    // one, which is what the relation lists and notes use.
                    let (login, id) = match found {
                        Some(player) => (player.login, Some(player.id)),
                        None => (login, None),
                    };
                    out.emit(SocialEvent::LoginLookedUp { login, id });
                }
                // Nothing stored, so the next menu opened on this name asks
                // again; until then its account entries stay unavailable.
                Err(error) => {
                    tracing::warn!(%login, %error, "could not look up an account by login")
                }
            }
        }
    }
}
