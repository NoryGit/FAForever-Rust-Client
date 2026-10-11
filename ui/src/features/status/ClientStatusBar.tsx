import { useEffect, useRef, useState } from "react";
import { Icon } from "../../design-system/Icon";
import { ipc } from "../../ipc/client";
import { useAppStore } from "../../store/store";
import type {
  AppCommand,
  AppState,
  ChatStatus,
  JoinState,
  LobbyStatus,
  ReplayDownloadStatus,
  UploadsState,
} from "../../ipc/bindings";
import { isUploadBusy } from "../../store/reducers/uploads";
import { plainError } from "../../shared/plainError";
import type { MessageKey } from "../../i18n";
import { useTranslation } from "../../i18n/useTranslation";
import "./status.css";

type ConnectionKind = "faf" | "chat";
type ConnectionStatus = ChatStatus | LobbyStatus;

const STATUS_LABEL = {
  disconnected: "status.connection.disconnected",
  connecting: "status.connection.connecting",
  connected: "status.connection.connected",
} as const satisfies Record<ConnectionStatus, MessageKey>;

export function GamePreparationStatus({
  state,
}: {
  state: Extract<JoinState, { type: "preparing" }>;
}) {
  const { t } = useTranslation();
  const progress = state.payload.progress === null
    ? null
    : Math.min(100, Math.max(0, state.payload.progress));

  return (
    <div className="client-status-task" aria-live="polite">
      <span
        className="client-status-task-label"
        title={t("status.matchSetup.title", { detail: state.payload.detail })}
      >
        <strong>{t("status.matchSetup.label")}</strong> {state.payload.detail}
      </span>
      <span
        className="client-status-progress"
        data-indeterminate={progress === null ? "true" : undefined}
        role="progressbar"
        aria-label={t("status.matchSetup.aria")}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={progress ?? undefined}
        aria-valuetext={progress === null ? state.payload.detail : `${state.payload.detail}, ${progress}%`}
      >
        <span style={progress === null ? undefined : { width: `${progress}%` }} />
      </span>
      <span className="client-status-task-percent">
        {progress === null ? t("status.active") : `${progress}%`}
      </span>
    </div>
  );
}

/**
 * The non-progress join phases, in the same slot as the preparation bar.
 *
 * These used to be an inline banner above the game list, which pushed the
 * workspace down for one line of text and put "Launching …" somewhere the eye
 * is not looking once the game is starting. The status bar already owns
 * long-running client state, so they belong beside it.
 */
export function GameJoinStatus({ state }: { state: JoinState }) {
  const { t } = useTranslation();
  const note = joinStatusNote(state, t);
  if (note === null) return null;
  return (
    <div className="client-status-task" aria-live="polite">
      <span className="client-status-task-label" title={state.type === "failed" ? state.payload.reason : note}>{note}</span>
    </div>
  );
}

type Translate = ReturnType<typeof useTranslation>["t"];

function joinStatusNote(state: JoinState, t: Translate): string | null {
  switch (state.type) {
    case "joining": return t("status.join.connecting", { id: state.payload.id });
    case "launched": return t("status.join.launched", { name: state.payload.launch.name });
    case "failed": return t("status.join.failed", { reason: plainError(state.payload.reason) });
    // In-game needs no narration, a launch failure is retained by the
    // notification centre where it can be dismissed, and a pending mod
    // replacement is already a modal the user is looking at.
    case "inGame":
    case "launchFailed":
    case "preparing":
    case "needsModReplacement":
    case "idle":
      return null;
  }
}

/**
 * Replay downloads use the same bottom task slot as match preparation. The
 * replay service cannot know a reliable total size through every CDN path, so
 * this deliberately stays indeterminate instead of showing a misleading
 * percentage.
 */
export function ReplayDownloadTask({
  status,
}: {
  status: Extract<ReplayDownloadStatus, { type: "downloading" }>;
}) {
  const { t } = useTranslation();
  const uid = status.payload.uid;
  return (
    <div className="client-status-task" aria-live="polite">
      <span className="client-status-task-label" title={t("status.replay.title", { uid })}>
        {/* The action leads, the subject follows, so the id is never left
            standing on its own as a bare number with no idea what it names. */}
        <strong>{t("status.replay.action")}</strong> {t("status.replay.subject", { uid })}
      </span>
      <span
        className="client-status-progress"
        data-indeterminate="true"
        role="progressbar"
        aria-label={t("status.replay.aria")}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuetext={t("status.active")}
      >
        <span />
      </span>
      <span className="client-status-task-percent">{t("status.active")}</span>
    </div>
  );
}

/**
 * A map or mod publish whose dialog was hidden. Hiding does not stop it, so
 * this is where it stays visible until the notification with the result.
 */
export function UploadTask({ status }: { status: UploadsState["status"] }) {
  const { t } = useTranslation();
  const progress =
    status.type === "compressing" && status.payload.totalBytes > 0
      ? Math.min(100, Math.floor((status.payload.doneBytes / status.payload.totalBytes) * 100))
      : status.type === "uploading" && status.payload.totalBytes > 0
        ? Math.min(100, Math.floor((status.payload.sentBytes / status.payload.totalBytes) * 100))
        : null;
  return (
    <div className="client-status-task" aria-live="polite">
      <span className="client-status-task-label">
        <strong>{t("status.upload.label")}</strong>
      </span>
      <span
        className="client-status-progress"
        data-indeterminate={progress === null ? "true" : undefined}
        role="progressbar"
        aria-label={t("status.upload.label")}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={progress ?? undefined}
      >
        <span style={progress === null ? undefined : { width: `${progress}%` }} />
      </span>
      <span className="client-status-task-percent">
        {progress === null ? t("status.active") : `${progress}%`}
      </span>
    </div>
  );
}

/**
 * Everything else the client is busy with, in the order it matters: installs
 * the player started first, then searches, then catalogues being read.
 *
 * One fixed place for "something is on its way", which is what this bar
 * already was for match preparation and replay downloads. The views used to
 * say it themselves, each in its own words and position, or not at all.
 * Failures stay in the views, beside the thing that failed and its Retry.
 *
 * Each value is read on its own, as a string or a boolean: a selector that
 * built the list would hand the store a new array every time and redraw the
 * bar on every state change.
 */
type BackgroundActivity = string | { label: string; cancel: AppCommand; progress?: number };

function useBackgroundActivities(): BackgroundActivity[] {
  const { t } = useTranslation();
  const mapInstall = useAppStore((s) => {
    const status = s.state.maps.installStatus;
    if (status.type !== "installing") return null;
    const folder = status.payload.folderName;
    return s.state.maps.vault.find((map) => map.folderName === folder)?.displayName ?? folder;
  });
  const modName = (mods: AppState["mods"], uid: string) =>
    mods.vault.find((mod) => mod.uid === uid)?.displayName
    ?? mods.installed.find((mod) => mod.uid === uid)?.displayName
    ?? uid;
  const modInstall = useAppStore((s) =>
    s.state.mods.installStatus.type === "installing"
      ? modName(s.state.mods, s.state.mods.installStatus.payload.uid)
      : null);
  const modToggle = useAppStore((s) =>
    s.state.mods.toggleStatus.type === "toggling"
      ? modName(s.state.mods, s.state.mods.toggleStatus.payload.uid)
      : null);
  // A map being generated, from wherever it was asked for: a replay card's
  // "+", the replay or game details, the Maps tab. Only the Maps tab showed
  // its progress, so generating from anywhere else ran for a minute with
  // nothing on screen saying so. A string, so the selector stays stable while
  // the download's byte count does not change the percentage.
  const mapGeneration = useAppStore((s) => {
    const status = s.state.mapGenerator.status;
    switch (status.type) {
      case "preparing":
      case "resolvingVersion":
        return t("replays.detail.preparingGenerator");
      case "downloading": {
        const { version, downloadedBytes, totalBytes } = status.payload;
        return totalBytes
          ? t("maps.generate.downloadingPercent", {
            version,
            percent: Math.min(100, Math.round((downloadedBytes / totalBytes) * 100)),
          })
          : t("maps.generate.downloading", { version });
      }
      case "generating":
        return t("lobby.details.generatingMap");
      default:
        return null;
    }
  });
  const mapSearch = useAppStore((s) => s.state.maps.browseStatus.type === "loading");
  const modSearch = useAppStore((s) => s.state.mods.browseStatus.type === "loading");
  const replaySearch = useAppStore((s) => s.state.replays.vaultStatus.type === "loading");
  const mapScan = useAppStore((s) => s.state.maps.installedStatus.type === "loading");
  const modScan = useAppStore((s) => s.state.mods.installedStatus.type === "loading");
  const mapVault = useAppStore((s) => s.state.maps.vaultStatus.type === "loading" ? s.state.maps.vaultProgress : undefined);
  const modVault = useAppStore((s) => s.state.mods.vaultStatus.type === "loading" ? s.state.mods.vaultProgress : undefined);
  const tutorial = useAppStore((s) => s.state.tutorials.launch.type === "preparing" ? s.state.tutorials.launch.payload.detail : null);
  const galacticWar = useAppStore((s) => s.state.galacticWar.status);
  const catalogue = (label: string, progress: AppState["maps"]["vaultProgress"], cancel: AppCommand): BackgroundActivity => ({
    label: progress ? t(progress.totalPages === null ? "status.activity.cataloguePages" : "status.activity.cataloguePagesTotal", {
      label, pages: progress.pages, total: progress.totalPages ?? 0,
    }) : label,
    cancel,
    progress: progress?.totalPages ? Math.min(100, Math.round(100 * progress.pages / progress.totalPages)) : undefined,
  });
  const leaderboards = useAppStore((s) =>
    s.state.leaderboard.catalogStatus.type === "loading"
    || s.state.leaderboard.ratingsStatus.type === "loading"
    || s.state.leaderboard.seasonStatus.type === "loading");
  const events = useAppStore((s) => s.state.events.status.type === "loading");
  const tournaments = useAppStore((s) => s.state.tourney.status.type === "loading");
  const changelog = useAppStore((s) => s.state.changelog.status.type === "loading");

  const cancellable: BackgroundActivity[] = [];
  if (tutorial !== null) cancellable.push({ label: tutorial, cancel: { kind: "Tutorials", command: { type: "cancelLaunch" } } });
  if (galacticWar.type === "downloading" || galacticWar.type === "installing") {
    cancellable.push({ label: `${t("lobby.galacticWar.short")}: ${t("lobby.galacticWar.action.working")}`, cancel: { kind: "GalacticWar", command: { type: "cancelInstall" } } });
  }
  if (mapVault !== undefined) cancellable.push(catalogue(t("maps.view.loadingVault"), mapVault, { kind: "Maps", command: { type: "cancelVaultLoad" } }));
  if (modVault !== undefined) cancellable.push(catalogue(t("mods.view.loadingVault"), modVault, { kind: "Mods", command: { type: "cancelVaultLoad" } }));
  return [...cancellable, ...[
    mapInstall !== null && t("status.activity.installingMap", { name: mapInstall }),
    modInstall !== null && t("status.activity.installingMod", { name: modInstall }),
    modToggle !== null && t("status.activity.togglingMod", { name: modToggle }),
    mapGeneration,
    mapSearch && t("maps.view.searching"),
    modSearch && t("mods.view.searching"),
    replaySearch && t("replays.vault.searching"),
    mapScan && t("maps.view.scanning"),
    modScan && t("mods.installed.scanning"),

    leaderboards && t("leaderboard.view.loadingCatalog"),
    events && t("events.loading"),
    tournaments && t("tournaments.loading"),
    changelog && t("changelog.loading"),
  ].filter((label): label is string => typeof label === "string")];
}

/** The first background activity, and how many more are running behind it. */
export function BackgroundActivityTask({ activities }: { activities: BackgroundActivity[] }) {
  const { t } = useTranslation();
  if (activities.length === 0) return null;
  const [activity, ...others] = activities;
  const labelOf = (item: BackgroundActivity) => typeof item === "string" ? item : item.label;
  const first = labelOf(activity);
  const rest = others.map(labelOf);
  const progress = typeof activity === "string" ? undefined : activity.progress;
  // The rest are named on hover rather than dropped: "+2" alone would say
  // that something is happening without saying what.
  const title = rest.length > 0 ? `${t("status.activity.alsoRunning")}: ${rest.join(", ")}` : first;
  return (
    <div className="client-status-task" aria-live="polite">
      <span className="client-status-task-label" title={title}>{first}</span>
      <span
        className="client-status-progress"
        data-indeterminate={progress === undefined ? "true" : undefined}
        role="progressbar"
        aria-label={first}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={progress}
        aria-valuetext={first}
      >
        <span style={progress === undefined ? undefined : { width: `${progress}%` }} />
      </span>
      <span className="client-status-task-percent" title={title}>
        {rest.length > 0 ? `+${rest.length}` : t("status.active")}
      </span>
      {typeof activity !== "string" && (
        <button type="button" className="client-status-task-action"
          aria-label={`${t("common.cancel")}: ${first}`} title={t("common.cancel")}
          onClick={() => ipc.send(activity.cancel)}>
          <Icon name="close" size={12} />
        </button>
      )}
    </div>
  );
}

/**
 * The matchmaker, wherever the player is in the client.
 *
 * Searching was only visible inside the matchmaker panel, so a ladder player
 * who queued and went to Chat or Replays had no sign it was still running and
 * no way to stop it short of going back. This is the fixed place for both.
 */
export function MatchmakingTask({
  state,
  queues,
}: {
  state: Exclude<AppState["lobby"]["matchmaking"], { type: "idle" } | { type: "cancelled" }>;
  queues: AppState["lobby"]["matchmakerQueues"];
}) {
  const { t } = useTranslation();
  // "2 vs 2", the way the queue cards name them, rather than "tmm2v2".
  const named = (queueName: string) => {
    const queue = queues.find((candidate) => candidate.queueName === queueName);
    return queue ? t("status.matchmaking.queue", { size: queue.teamSize }) : queueName;
  };
  if (state.type === "matchFound") {
    return (
      <div className="client-status-task is-attention" role="status" aria-live="assertive">
        <span className="client-status-task-label">
          <strong>{t("status.matchmaking.found", { queue: named(state.payload.queueName) })}</strong>
        </span>
      </div>
    );
  }
  // Preparing (#390) is the first half of a search: the featured mod and the
  // pool maps come down before the server is asked. It can be stopped the
  // same way, as the matchmaker panel does.
  const searching = state.type === "searching" || state.type === "preparing";
  const label = state.type === "preparing"
    ? t("lobby.matchmaker.summary.preparing")
    : state.type === "searching"
      ? t("status.matchmaking.searching", { queues: state.payload.queueNames.map(named).join(", ") })
      : t("status.matchmaking.launching", { queue: named(state.payload.queueName) });
  const stop = () => {
    if (state.type !== "searching" && state.type !== "preparing") return;
    state.payload.queueNames.forEach((queueName) =>
      ipc.send({ kind: "Lobby", command: { type: "matchmake", payload: { queueName, start: false } } }),
    );
  };
  // No progress bar: a search has no progress to measure, and an endless
  // sweep in the corner where downloads report theirs read as one that never
  // finished. The Play tab's dot says it instead, and the same dot leads the
  // line here, coloured by the same states.
  return (
    <div className="client-status-task" aria-live="polite">
      <i className="client-status-search-dot" data-state={state.type} aria-hidden="true" />
      <span className="client-status-task-label" title={label}>{label}</span>
      {searching && (
        <button
          type="button"
          className="client-status-task-action"
          onClick={stop}
          aria-label={t("lobby.matchmaker.stopSearching")}
          title={t("lobby.matchmaker.stopSearching")}
        >
          <Icon name="close" size={12} />
        </button>
      )}
    </div>
  );
}

export function ClientStatusBar() {
  const { t } = useTranslation();
  const session = useAppStore((state) => state.state.session);
  const player = useAppStore((state) => state.state.auth.player);
  const lobbyStatus = useAppStore((state) => state.state.lobby.status);
  const joinState = useAppStore((state) => state.state.lobby.join);
  const replayDownloadStatus = useAppStore((state) => state.state.replays.downloadStatus);
  const chatStatus = useAppStore((state) => state.state.chat.status);
  const uploads = useAppStore((state) => state.state.uploads);
  const hiddenUpload = uploads.request === null && isUploadBusy(uploads.status);
  const activities = useBackgroundActivities();
  const matchmaking = useAppStore((state) => state.state.lobby.matchmaking);
  const matchmakerQueues = useAppStore((state) => state.state.lobby.matchmakerQueues);
  const [openMenu, setOpenMenu] = useState<ConnectionKind | null>(null);
  const rootRef = useRef<HTMLElement>(null);
  const joinTaskVisible = joinState.type === "joining"
    || joinState.type === "launched"
    || joinState.type === "failed";

  useEffect(() => {
    if (!openMenu) return;

    const closeOnOutsideClick = (event: MouseEvent) => {
      if (event.target instanceof Node && !rootRef.current?.contains(event.target)) {
        setOpenMenu(null);
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpenMenu(null);
    };

    document.addEventListener("mousedown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("mousedown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [openMenu]);

  const reconnect = async (kind: ConnectionKind) => {
    setOpenMenu(null);
    if (kind === "faf") {
      if (lobbyStatus === "disconnected") {
        await ipc.dispatch({ kind: "Lobby", command: { type: "connect" } });
      } else {
        await ipc.dispatch({ kind: "Lobby", command: { type: "disconnect" } });
      }
      return;
    }

    if (chatStatus === "disconnected") {
      if (player?.name) {
        await ipc.dispatch({ kind: "Chat", command: { type: "connect", payload: { username: player.name } } });
      }
    } else {
      await ipc.dispatch({ kind: "Chat", command: { type: "disconnect" } });
    }
  };

  const renderConnectionMenu = (kind: ConnectionKind, status: ConnectionStatus, label: string) => {
    const isOpen = openMenu === kind;
    const canConnect = kind !== "chat" || Boolean(player?.name);
    const actionLabel = status === "disconnected" ? t("status.reconnect") : t("status.disconnect");
    const stateLabel = t(STATUS_LABEL[status]);

    return (
      <div className="client-status-menu" key={kind}>
        <button
          type="button"
          className="client-status-connection"
          data-status={status}
          aria-expanded={isOpen}
          aria-haspopup="menu"
          aria-controls={`client-status-menu-${kind}`}
          onClick={() => setOpenMenu(isOpen ? null : kind)}
        >
          <i aria-hidden="true" />
          <span>{t("status.connection.summary", { service: label, state: stateLabel })}</span>
          <span className="client-status-chevron" aria-hidden="true" />
        </button>
        {isOpen && (
          <div className="client-status-popover" id={`client-status-menu-${kind}`} role="menu">
            <div className="client-status-popover-heading">
              <span className="client-status-popover-dot" data-status={status} aria-hidden="true" />
              <span>{t("status.connection.heading", { service: label })}</span>
            </div>
            {/* The state and the thing you can do about it, side by side and the
                same size. They used to be a word tucked into the heading and a
                full-width menu row, which read as one item with a caption: the
                pair is what the popover is actually for. */}
            <div className="client-status-popover-row">
              <span className="client-status-state" data-status={status}>{stateLabel}</span>
              <button
                type="button"
                className="client-status-action"
                role="menuitem"
                disabled={!canConnect}
                onClick={() => void reconnect(kind)}
              >
                {actionLabel}
              </button>
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <footer ref={rootRef} className="client-status-bar" aria-label={t("status.bar.aria")}>
      <span className="client-status-version">v{session.backendVersion || "0.8.0"}</span>
      {joinState.type === "preparing"
        ? <GamePreparationStatus state={joinState} />
        : joinTaskVisible
          ? <GameJoinStatus state={joinState} />
          : matchmaking.type !== "idle" && matchmaking.type !== "cancelled"
            ? <MatchmakingTask state={matchmaking} queues={matchmakerQueues} />
          : replayDownloadStatus.type === "downloading"
            ? <ReplayDownloadTask status={replayDownloadStatus} />
            : hiddenUpload
              ? <UploadTask status={uploads.status} />
              : activities.length > 0
                ? <BackgroundActivityTask activities={activities} />
                : null}
      <div className="client-status-connections">
        {renderConnectionMenu("faf", lobbyStatus, "FAF")}
        {renderConnectionMenu("chat", chatStatus, t("status.service.chat"))}
      </div>
    </footer>
  );
}
