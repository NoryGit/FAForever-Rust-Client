// Lessons: the entries the client can actually start.
//
// Narrowed on purpose. FAF's tutorial API returns two different things under
// one name: a handful of playable scenarios, and whole categories ("Video
// tutorials", "Written guides") that are links to YouTube and the wiki. Both
// used to be listed here, which meant most of the tab was rows that opened a
// browser under a heading promising a lesson.
//
// Nothing mounts this today. FAF's tutorial API is no longer read by the
// training tab at all: it flags entries playable whose maps no longer start
// anything, and its "Video tutorials" and "Written guides" categories are links
// rather than lessons, none of which could be corrected without a client
// release. The lessons section shows an empty state instead, and everything a
// player reads comes from the catalogue repository.
//
// The file stays because the launch path is finished and correct: it patches
// the featured mod, fetches the map and opens an offline game. The day somebody
// authors a real scenario, this is the pane that shows it, and the work of
// getting there is content rather than code.
//
// Mirrors the Java client's `TutorialController`/`tutorial_detail.fxml`:
// categories down the left, and a detail pane where the briefing reads as prose
// beside a fixed-size map thumbnail with the launch button under it. The image
// is deliberately *not* a banner: the API serves small map previews, and
// stretching one across the pane produced a wall of blur.

import { useEffect, useMemo } from "react";
import { Button } from "../../design-system/Button";
import { Icon } from "../../design-system/Icon";
import type { Tutorial } from "../../ipc/bindings";
import { ipc } from "../../ipc/client";
import { loadStatusNote } from "../../shared/loadStatusNote";
import { useAppStore } from "../../store/store";
import "./tutorials.css";
import { useTranslation } from "../../i18n/useTranslation";

const load = () => ipc.send({ kind: "Tutorials", command: { type: "load" } });
const select = (tutorialId: number) =>
  ipc.send({ kind: "Tutorials", command: { type: "select", payload: { tutorialId } } });
const launch = (tutorialId: number) =>
  ipc.send({ kind: "Tutorials", command: { type: "launch", payload: { tutorialId } } });

export function TutorialsView() {
  const { t } = useTranslation();
  const state = useAppStore((store) => store.state.tutorials);

  useEffect(() => {
    if (useAppStore.getState().state.tutorials.status.type === "idle") void load();
  }, []);

  // Everything below reads from this rather than from the slice, so the count,
  // the grouping, the empty state and the detail pane cannot disagree about
  // what the tab contains.
  const playable = useMemo(() => state.tutorials.filter(isPlayable), [state.tutorials]);
  const selected = playable.find((t) => t.id === state.selectedId) ?? null;
  const note = loadStatusNote(state.status, t("tutorials.loading"), t("tutorials.loadFailed"));

  // Group by category, keeping each author's teaching order (`ordinal`).
  const groups = useMemo(() => {
    const byCategory = state.categories.map((category) => ({
      category,
      tutorials: playable
        .filter((tutorial) => tutorial.categoryId === category.id)
        .sort((a, b) => a.ordinal - b.ordinal || a.title.localeCompare(b.title)),
    }));

    // A lesson the API left uncategorised still has to be reachable.
    const ungrouped = playable
      .filter((tutorial) => tutorial.categoryId === null)
      .sort((a, b) => a.ordinal - b.ordinal || a.title.localeCompare(b.title));

    return [
      ...byCategory.filter((group) => group.tutorials.length > 0),
      ...(ungrouped.length > 0 ? [{ category: null, tutorials: ungrouped }] : []),
    ];
  }, [state.categories, playable]);

  const total = groups.reduce((sum, group) => sum + group.tutorials.length, 0);
  const selectedCategory = selected
    ? state.categories.find((category) => category.id === selected.categoryId)?.name ?? t("tutorials.otherLessons")
    : "";

  return (
    <div className="tutorials-view">
      <header className="tutorials-header">
        <div>
          <span className="tutorials-eyebrow">{t("tutorials.eyebrow")}</span>
          <h2>{t("tutorials.tutorials")}</h2>
        </div>
        {total > 0 && (
          <span className="muted tutorials-count">
            {t("tutorials.lessonCount", { count: total })}
          </span>
        )}
      </header>

      {note && (
        // Retry lives here rather than as a permanent toolbar button: the list
        // is a near-static catalogue loaded once, so a refresh control only has
        // a job when the load actually failed.
        <p className="surface tutorials-state muted">
          <span>{note}</span>
          {state.status.type === "failed" && (
            <Button onClick={() => void load()}>
              <Icon name="refresh" size={15} /> {t("tutorials.tryAgain")}
            </Button>
          )}
        </p>
      )}

      {state.status.type === "ready" && total === 0 && (
        <p className="surface tutorials-state muted">
          <span>{t("tutorials.none")}</span>
        </p>
      )}

      {total > 0 && (
        <div className="tutorials-body">
          <nav className="surface-panel tutorials-list" aria-label={t("tutorials.tutorials")}>
            {groups.map((group) => (
              <div className="tutorials-group" key={group.category?.id ?? "other"}>
                <h3 className="tutorials-group-name">
                  <span>{group.category?.name ?? t("tutorials.otherLessons")}</span>
                  <span>{group.tutorials.length}</span>
                </h3>
                {group.tutorials.map((tutorial) => (
                  <button
                    type="button"
                    key={tutorial.id}
                    className={
                      tutorial.id === state.selectedId
                        ? "tutorial-row is-active"
                        : "tutorial-row"
                    }
                    aria-current={tutorial.id === state.selectedId}
                    onClick={() => void select(tutorial.id)}
                  >
                    {tutorial.imageUrl ? (
                      <img className="tutorial-row-thumb" src={tutorial.imageUrl} alt="" loading="lazy" />
                    ) : (
                      <TutorialRowMark />
                    )}
                    <span className="tutorial-row-copy">
                      <strong>{tutorial.title || t("tutorials.untitled")}</strong>
                    </span>
                  </button>
                ))}
              </div>
            ))}
          </nav>

          {selected ? (
            <TutorialDetail tutorial={selected} categoryName={selectedCategory} />
          ) : (
            <p className="surface tutorials-state muted"><span>{t("tutorials.select")}</span></p>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * Twin of `Tutorial::is_playable` in the domain. The server's `launchable`
 * flag is not enough on its own: without a map there is nothing to download,
 * and without a scenario name the game opens to its main menu: which looks
 * exactly like the client having done nothing.
 */
function isPlayable(tutorial: Tutorial): boolean {
  return tutorial.launchable && tutorial.mapFolderName !== "" && tutorial.technicalName !== "";
}

/** A lesson, as a mark, for a row whose map has no preview image. */
function TutorialRowMark() {
  const { t } = useTranslation();
  return (
    <span className="tutorial-row-mark is-playable" title={t("tutorials.playableLesson")}>
      <Icon name="play" size={13} />
    </span>
  );
}

function TutorialDetail({ tutorial, categoryName }: { tutorial: Tutorial; categoryName: string }) {
  const { t } = useTranslation();
  const launchState = useAppStore((store) => store.state.tutorials.launch);

  // Only narrate progress for *this* lesson: the status is global, and
  // another lesson's "Updating tutorials…" under this title would be wrong.
  const preparing =
    launchState.type === "preparing" && launchState.payload.tutorialId === tutorial.id
      ? launchState.payload.detail
      : null;
  const launched =
    launchState.type === "launched" && launchState.payload.tutorialId === tutorial.id;
  // Keyed the same way: one lesson's failure under every lesson opened after
  // it read as though each of them had failed too.
  const failure =
    launchState.type === "failed" && launchState.payload.tutorialId === tutorial.id
      ? launchState.payload.reason
      : null;

  return (
    <section className="surface-panel tutorial-detail">
      <div className="tutorial-detail-body">
        <div className="tutorial-detail-copy">
          <div className="tutorial-detail-kicker">
            <span>{categoryName}</span>
          </div>
          <h3>{tutorial.title || t("tutorials.untitled")}</h3>
          {tutorial.description
            ? <p className="tutorial-brief">{tutorial.description}</p>
            : <p className="muted tutorial-brief">{t("tutorials.noBriefing")}</p>}

          <dl className="tutorial-meta">
              <div><dt>{t("tutorials.map")}</dt><dd>{tutorial.mapFolderName}</dd></div>
              <div><dt>{t("tutorials.mode")}</dt><dd>{t("tutorials.offline")}</dd></div>
          </dl>

          {/* Patching the tutorials mod and fetching the map is slow the first
              time; a silent client looks broken. */}
          {preparing !== null && <p className="muted tutorial-progress">{preparing}</p>}
          {launched && <p className="tutorial-progress is-ok">{t("tutorials.launched")}</p>}
          {failure !== null && <p className="tutorial-progress is-error">{failure}</p>}

          <div className="tutorial-actions">
            <Button
              variant="primary"
              disabled={preparing !== null}
              title={t("tutorials.play", { title: tutorial.title })}
              onClick={() => void launch(tutorial.id)}
            >
              {preparing !== null ? (
                t("tutorials.preparing")
              ) : (
                <>
                  <Icon name="play" size={16} /> {t("tutorials.start")}
                </>
              )}
            </Button>
            {preparing !== null && (
              <Button onClick={() => ipc.send({ kind: "Tutorials", command: { type: "cancelLaunch" } })}>
                {t("common.cancel")}
              </Button>
            )}
            <span className="muted tutorial-launch-note">{t("tutorials.autoPrepared")}</span>
          </div>
        </div>

        <aside className="tutorial-detail-side">
          {tutorial.imageUrl ? (
            <img className="tutorial-art" src={tutorial.imageUrl} alt="" loading="lazy" />
          ) : (
            <div className="tutorial-art tutorial-art-empty" aria-hidden>
              <Icon name="maps" size={28} />
            </div>
          )}

        </aside>
      </div>
    </section>
  );
}
