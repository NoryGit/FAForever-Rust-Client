// What a `faf://` link opens, once the backend has read it.
//
// The backend parses every link, from chat or from the operating system, and
// puts the result in `nav.linkRequest` (faf-domain's `protocol::deep_link`).
// Opening it is the frontend's half, because several tabs open on one item
// through state that lives in the page: the map and mod vaults through their
// focus requests, the replay vault through a search request. So the shell
// watches for a request, opens it the way the tab's own buttons do, and says
// it has, which clears it.
//
// Nothing here starts a game or a download: a replay link opens the replay's
// entry in the vault, a map link the map's.

import { useEffect } from "react";
import type { AppCommand, DeepLink } from "../ipc/bindings";
import { ipc } from "../ipc/client";
import { useAppStore } from "../store/store";
import { requestMapVaultFocus } from "./mapVaultFocus";
import { requestModVaultFocus } from "./modVaultFocus";
import { EMPTY_REPLAY_QUERY } from "./replayQuery";
import { requestReplaySearch } from "./replaySearchIntent";

/** Ask the backend to open a link, from chat or anywhere else in the page. */
export function openLink(url: string) {
  ipc.send({ kind: "Nav", command: { type: "openLink", payload: { url } } });
}

/**
 * What to type into the map vault's search for a folder name.
 *
 * The vault searches display names, and a folder only spells one roughly:
 * `setons_clutch.v0004` is "Seton's Clutch", and the search glob would not
 * find "setons clutch" in it. So the search is the folder's longest word,
 * which the display name contains in nearly every case, and the vault then
 * selects the exact version among the results by its folder.
 */
export function mapSearchTerm(folderName: string): string {
  const base = folderName.replace(/\.v\d+$/i, "");
  const words = base.split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  // A tie goes to the later word: a possessive comes first more often than
  // not, and "setons" is not in "Seton's".
  return words.reduce((longest, word) => (word.length >= longest.length ? word : longest), "") || base;
}

/** Open what a link points at, the way that tab's own buttons do. */
async function showLink(link: DeepLink, isCurrent: () => boolean): Promise<void> {
  const settle = (command: AppCommand) => isCurrent() ? ipc.settle(command) : Promise.resolve();
  switch (link.type) {
    case "tourney":
      await settle({ kind: "Nav", command: { type: "select", payload: { tab: "tournaments" } } });
      await settle({ kind: "Tourney", command: { type: "select", payload: { tournamentId: link.payload.id } } });
      return;
    case "map":
      await settle({ kind: "Nav", command: { type: "selectMapsSection", payload: { section: "vault" } } });
      if (!isCurrent()) return;
      requestMapVaultFocus({ name: mapSearchTerm(link.payload.folderName), folderName: link.payload.folderName });
      await settle({ kind: "Nav", command: { type: "select", payload: { tab: "maps" } } });
      return;
    case "mod":
      await settle({ kind: "Nav", command: { type: "selectModsSection", payload: { section: "vault" } } });
      if (!isCurrent()) return;
      // A uid is found by the search that looks beyond names.
      requestModVaultFocus(link.payload.uid, "description");
      await settle({ kind: "Nav", command: { type: "select", payload: { tab: "mods" } } });
      return;
    case "replay":
      await settle({ kind: "Nav", command: { type: "selectReplaysSection", payload: { section: "online" } } });
      if (!isCurrent()) return;
      requestReplaySearch({ ...EMPTY_REPLAY_QUERY, replayId: String(link.payload.id) });
      await settle({ kind: "Nav", command: { type: "select", payload: { tab: "replays" } } });
      return;
    case "changelog":
      await settle({ kind: "Nav", command: { type: "selectChangelogSection", payload: { section: "game" } } });
      await settle({ kind: "Nav", command: { type: "select", payload: { tab: "changelog" } } });
      await settle({ kind: "Changelog", command: { type: "select", payload: { id: link.payload.release } } });
      return;
    case "unit":
      await settle({ kind: "Nav", command: { type: "selectChangelogSection", payload: { section: "units" } } });
      await settle({ kind: "Nav", command: { type: "select", payload: { tab: "changelog" } } });
      await settle({ kind: "UnitHistory", command: { type: "select", payload: { id: link.payload.id } } });
      return;
  }
}

const LINK_RETRY_DELAYS = [250, 1_000, 3_000];

/**
 * Open each link request once. Mounted in the shell, which only exists once
 * somebody is signed in: a link that started the client waits through the
 * sign-in rather than being opened behind the login screen and forgotten.
 */
export function useLinkRequests() {
  // Nav updates can carry another copy of the same request while it opens.
  const requestId = useAppStore((state) => state.state.nav.linkRequest?.id);
  useEffect(() => {
    const request = useAppStore.getState().state.nav.linkRequest;
    if (!request || request.id !== requestId) return;
    const { id, link } = request;
    let disposed = false;
    let opened = false;
    let failures = 0;
    let timer: number | undefined;
    const isCurrent = () => !disposed && useAppStore.getState().state.nav.linkRequest?.id === id;
    const deliver = async () => {
      if (!isCurrent()) return;
      try {
        if (!opened) {
          await showLink(link, isCurrent);
          opened = true;
        }
        if (isCurrent()) await ipc.settle({ kind: "Nav", command: { type: "linkShown", payload: { id } } });
      } catch (error) {
        if (!isCurrent()) return;
        const delay = LINK_RETRY_DELAYS[failures++];
        if (delay === undefined) throw error;
        timer = window.setTimeout(() => ipc.run(deliver()), delay);
      }
    };
    ipc.run(deliver());
    return () => {
      disposed = true;
      window.clearTimeout(timer);
    };
  }, [requestId]);
}
