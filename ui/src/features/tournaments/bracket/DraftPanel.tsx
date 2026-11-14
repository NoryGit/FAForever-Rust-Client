// The captains draft: who is on the clock, and who is left to pick.
//
// The order is the service's and is walked, not rebuilt: captains pick
// concurrently, so a locally computed turn would disagree with whoever picked
// last. Everything here reads `event.draft`.
//
// Two states, and they are different screens rather than the same one greyed
// out: before the draft, an organiser chooses the captains, by hand or as the
// top so many by rating; during it, whoever is on the clock picks from the
// pool.

import { useState, type ReactNode } from "react";
import { Button } from "../../../design-system/Button";
import type { CaptainMode, PlayerSummary, Tourney, TourneyPlayer, TourneyTeam } from "../../../ipc/bindings";
import type { OrganiserActions, TeamActions } from "../tourneyActions";
import { useTranslation } from "../../../i18n/useTranslation";
import { EntrantName } from "../EntrantName";
import { PlayerChip } from "../PlayerChip";
import { PlayerTable } from "../PlayerTable";
import {
  draftTurn,
  isLegalFrom,
  mayPick,
  mayUndoPick,
  profileOf,
  undrafted,
} from "../../../shared/rules/tourneyRules";

interface DraftPanelProps {
  event: Tourney;
  profiles: PlayerSummary[];
  busy: boolean;
  /** Picking, undoing a pick, and naming the captains. */
  teams: Pick<TeamActions, "draftPick" | "draftUndo" | "setCaptains">;
  /** Starting the draft, and the organiser's single-call changes. */
  organiser: Pick<OrganiserActions, "advance" | "admin">;
}

export function DraftPanel(props: DraftPanelProps) {
  const { event, busy } = props;
  const { t } = useTranslation();
  const [captains, setCaptains] = useState<string[]>(event.pendingCaptains);
  const [mode, setMode] = useState<CaptainMode>(event.captainMode);
  const [count, setCount] = useState(event.captainCount >= 2 ? String(event.captainCount) : "");

  // A name is the way to the player's card, here as everywhere else in the tab.
  const nameOf = (player: TourneyPlayer) => (
    <EntrantName name={player.name} fafId={player.fafId} profile={profileOf(props.profiles, player)} />
  );
  // Inside a checkbox label a button would take the click the box should, so
  // the captains list keeps the plain chip.
  const chipOf = (player: TourneyPlayer) => {
    const profile = profileOf(props.profiles, player);
    return profile ? <PlayerChip player={profile} overrideName={player.name} /> : player.name;
  };

  const teamName = (teamId: string) => {
    const team = event.teams.find((held) => held.id === teamId);
    const named = team?.name.trim() ?? "";
    if (named !== "") return named;
    const captain = event.players.find((player) => player.id === team?.captainId);
    return captain?.name ?? teamId;
  };

  // Before it starts: an organiser marks the captains and closes signups.
  if (event.draft === null) {
    if (!event.viewer.organiser || !isLegalFrom("startDraft", event.status)) return null;
    const eligible = event.players.filter((player) => !player.pending);
    // Worked out by the service at the moment the draft starts, so this is a
    // preview: late signups and rating corrections still move it.
    const ranked = [...eligible].sort((left, right) => (right.rating ?? 0) - (left.rating ?? 0));
    const wanted = Number.parseInt(count, 10);
    const validCount = Number.isInteger(wanted) && wanted >= 2 && wanted <= 64;
    const teams = mode === "rating" ? (validCount ? wanted : 0) : captains.length;
    const ready = mode === "rating" ? validCount && ranked.length >= wanted : captains.length >= 2;
    const saveMode = (next: CaptainMode, typed: string) => {
      const parsed = Number.parseInt(typed, 10);
      props.organiser.admin({
        type: "setCaptainMode",
        payload: { mode: next, count: Number.isInteger(parsed) ? parsed : 0 },
      });
    };
    return (
      <section className="tournament-draft">
        <h5>{t("tournaments.draft.captainsHeading")}</h5>
        <p className="muted">
          {t("tournaments.draft.captainsAreTeams")}{" "}
          {t(event.draftSnakes ? "tournaments.draft.orderSnake" : "tournaments.draft.orderLinear")}{" "}
          {t("tournaments.draft.teamOf", { size: event.teamSize })}
        </p>
        <label className="tournament-field">
          <span>{t("tournaments.draft.howChosen")}</span>
          <select
            value={mode}
            disabled={busy}
            onChange={(changed) => {
              const next = changed.target.value as CaptainMode;
              setMode(next);
              saveMode(next, count);
            }}
          >
            <option value="manual">{t("tournaments.draft.modeManual")}</option>
            <option value="rating">{t("tournaments.draft.modeRating")}</option>
          </select>
        </label>
        {mode === "rating" ? (
          <>
            <label className="tournament-field">
              <span>{t("tournaments.draft.howMany")}</span>
              <input
                type="number"
                min={2}
                max={64}
                value={count}
                placeholder="8"
                onChange={(changed) => setCount(changed.target.value)}
                onBlur={() => {
                  if (validCount && wanted !== event.captainCount) saveMode(mode, count);
                }}
              />
              <small className="muted">{t("tournaments.draft.howManyHint")}</small>
            </label>
            <p className="muted">
              {!validCount
                ? t("tournaments.draft.previewEnter")
                : ranked.length < wanted
                  ? t("tournaments.draft.previewShort", { have: ranked.length, more: wanted - ranked.length })
                  : t("tournaments.draft.previewWould", {
                      names: ranked
                        .slice(0, wanted)
                        .map((player) => `${player.name} (${player.rating ?? "–"})`)
                        .join(", "),
                    })}
            </p>
          </>
        ) : (
          <p className="muted">{t("tournaments.draft.captainsHint")}</p>
        )}
        {mode === "manual" && (
          <ul className="tournament-entrant-list">
            {eligible.map((player) => (
              <li className="tournament-entrant" key={player.id}>
                <label className="tournament-checkbox">
                  <input
                    type="checkbox"
                    checked={captains.includes(player.id)}
                    onChange={() =>
                      setCaptains((held) =>
                        held.includes(player.id)
                          ? held.filter((id) => id !== player.id)
                          : [...held, player.id],
                      )
                    }
                  />
                  <span>{chipOf(player)}</span>
                </label>
                {player.rating !== null && <span className="muted">{player.rating}</span>}
              </li>
            ))}
          </ul>
        )}
        {teams >= 2 && (
          <p className="muted">
            {t("tournaments.draft.bracketPreview", { teams, size: event.teamSize, needed: teams * event.teamSize })}
            {eligible.length < teams * event.teamSize &&
              ` ${t("tournaments.draft.moreNeeded", { have: eligible.length, more: teams * event.teamSize - eligible.length })}`}
          </p>
        )}
        <div className="tournament-detail-actions">
          {mode === "manual" && (
            <Button disabled={busy} onClick={() => props.teams.setCaptains(captains)}>
              {t("tournaments.draft.saveCaptains")}
            </Button>
          )}
          {/* The service wants at least two, and says so. Checking here keeps
              the refusal off a button that looks ready. */}
          <Button
            variant="primary"
            disabled={busy || !ready}
            onClick={() => props.organiser.advance("startDraft")}
          >
            {t("tournaments.draft.start")}
          </Button>
        </div>
      </section>
    );
  }

  const turn = draftTurn(event);
  const pool = undrafted(event);
  const mine = mayPick(event);
  // Who picks after the team on the clock, so a captain can see their turn
  // coming. The service's order, walked as it is.
  const upNext = event.draft.order
    .slice(event.draft.current + 1, event.draft.current + 1 + UP_NEXT)
    .map(teamName);
  const teams = draftTeams(event);

  return (
    <section className="tournament-draft">
      <header className="tournament-draft-head">
        <h5>{t("tournaments.draft.heading")}</h5>
        {turn === null ? (
          <span className="muted">{t("tournaments.draft.finished")}</span>
        ) : (
          <span>
            {t("tournaments.draft.onTheClock", { team: teamName(turn) })}
            {mine && <strong> {t("tournaments.draft.yours")}</strong>}
            <span className="muted">
              {" "}
              {t("tournaments.draft.remaining", {
                count: event.draft.order.length - event.draft.current,
              })}
            </span>
          </span>
        )}
        {mayUndoPick(event) && (
          <Button disabled={busy} onClick={props.teams.draftUndo}>
            {t("tournaments.draft.undo")}
          </Button>
        )}
      </header>
      {turn !== null && upNext.length > 0 && (
        <p className="muted tournament-draft-next">
          {t("tournaments.draft.upNext", { teams: upNext.join(" → ") })}
        </p>
      )}

      {/* What every captain has so far: the thing a pick is weighed against. */}
      <ul className="tournament-team-grid tournament-draft-teams">
        {teams.map((team) => (
          <DraftTeamCard
            key={team.id}
            event={event}
            team={team}
            name={teamName(team.id)}
            onClock={team.id === turn}
            nameOf={nameOf}
          />
        ))}
      </ul>

      <h5 className="tournament-draft-pool-heading">
        {t("tournaments.draft.poolHeading", { count: pool.length })}
      </h5>
      {pool.length === 0 ? (
        <p className="muted">{t("tournaments.draft.poolEmpty")}</p>
      ) : (
        // Ranked by the event's own rating, highest first, with every name a
        // way to the player's card. The Pick is live only for whoever is on
        // the clock; everyone else reads the same pool, which is how a draft
        // is followed.
        <PlayerTable
          event={event}
          profiles={props.profiles}
          players={pool}
          action={
            mine
              ? {
                  heading: t("tournaments.draft.pick"),
                  render: (player) => (
                    <Button disabled={busy} onClick={() => props.teams.draftPick(player.id)}>
                      {t("tournaments.draft.pick")}
                    </Button>
                  ),
                }
              : undefined
          }
        />
      )}
    </section>
  );
}

/** How many teams after the one on the clock the draft names. */
const UP_NEXT = 3;

/**
 * The teams in the order they first pick: the order captains read the draft
 * in. A team the order never names (none, in a well-formed draft) goes last.
 */
function draftTeams(event: Tourney): TourneyTeam[] {
  const order = event.draft?.order ?? [];
  const first = (team: TourneyTeam) => {
    const at = order.indexOf(team.id);
    return at === -1 ? Number.MAX_SAFE_INTEGER : at;
  };
  return [...event.teams].sort((left, right) => first(left) - first(right));
}

/** The mean of the ratings that are there, rounded; `null` when none is. */
function averageRating(players: TourneyPlayer[]): number | null {
  const rated = players.flatMap((player) => (player.rating === null ? [] : [player.rating]));
  if (rated.length === 0) return null;
  return Math.round(rated.reduce((sum, rating) => sum + rating, 0) / rated.length);
}

interface DraftTeamCardProps {
  event: Tourney;
  team: TourneyTeam;
  name: string;
  onClock: boolean;
  nameOf: (player: TourneyPlayer) => ReactNode;
}

/** One captain's team so far: who is on it, how strong, and its open places. */
function DraftTeamCard({ event, team, name, onClock, nameOf }: DraftTeamCardProps) {
  const { t } = useTranslation();
  const members = team.playerIds.flatMap((id) => {
    const player = event.players.find((held) => held.id === id);
    return player === undefined ? [] : [player];
  });
  // Captain first, then in the order they were picked.
  const roster = [
    ...members.filter((player) => player.id === team.captainId),
    ...members.filter((player) => player.id !== team.captainId),
  ];
  const average = averageRating(roster);
  const open = Math.max(0, event.teamSize - roster.length);
  const classes = [
    "tournament-team-card",
    onClock ? "is-on-clock" : "",
    team.id === event.viewer.memberTeamId ? "is-mine" : "",
  ].filter((name) => name !== "");
  return (
    <li className={classes.join(" ")}>
      <div className="tournament-team-card-head">
        <div className="tournament-team-card-title">
          <span className="tournament-team-name">{name}</span>
          {onClock && <span className="tournament-badge is-running">{t("tournaments.draft.picking")}</span>}
        </div>
        <div className="tournament-team-card-sub tournament-team-rating">
          <span className="mono">{t("tournaments.teams.size", { have: roster.length, want: event.teamSize })}</span>
          {average !== null && <span className="mono">{t("tournaments.draft.average", { rating: average })}</span>}
        </div>
      </div>
      <ul className="tournament-draft-roster">
        {roster.map((player) => (
          <li key={player.id}>
            <span className="tournament-entrant-name">
              {nameOf(player)}
              {player.id === team.captainId && (
                <span className="tournament-badge">{t("tournaments.teams.captain")}</span>
              )}
            </span>
            <span className="mono muted">{player.rating ?? "–"}</span>
          </li>
        ))}
        {Array.from({ length: open }, (_, index) => (
          <li key={`open-${index}`} className="is-open muted">
            {t("tournaments.draft.openPlace")}
          </li>
        ))}
      </ul>
    </li>
  );
}
