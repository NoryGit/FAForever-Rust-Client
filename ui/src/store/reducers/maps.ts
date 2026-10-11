import type { MapsEvent, MapsState } from "../../ipc/bindings";

/**
 * Twin of `MAX_LOCAL_PREVIEWS` in crates/faf-domain/src/state/maps.rs, where
 * the number is justified against measured preview sizes.
 */
const MAX_LOCAL_PREVIEWS = 128;

export function reduceMaps(state: MapsState, event: MapsEvent): MapsState {
  switch (event.type) {
    case "vaultProgress":
      return { ...state, vaultProgress: event.payload.progress };
    case "vaultCancelled":
      return { ...state, vaultStatus: { type: "cancelled" }, vaultProgress: null };
    case "vaultLoading":
      return { ...state, vaultStatus: { type: "loading" }, vaultProgress: null };
    // Merged with what folder lookups already added: see the Rust twin.
    case "vaultLoaded": {
      const loaded = new Set(event.payload.maps.map((map) => map.versionId));
      const resolved = state.vault.filter((map) => !loaded.has(map.versionId));
      return { ...state, vault: [...event.payload.maps, ...resolved], vaultStatus: { type: "ready" } };
    }
    case "vaultLoadFailed":
      return { ...state, vaultStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    // Appended, never replacing: these are records the catalogue leaves out
    // (withdrawn versions), looked up one folder at a time.
    case "vaultFoldersResolved": {
      const known = new Set(state.vault.map((map) => map.versionId));
      const added = event.payload.maps.filter((map) => {
        if (known.has(map.versionId)) return false;
        known.add(map.versionId);
        return true;
      });
      return added.length === 0 ? state : { ...state, vault: [...state.vault, ...added] };
    }
    // `browse` is one page of a server-side search and is separate from
    // `vault`, which stays the whole catalogue used as a folder-name index.
    case "vaultSearching":
      return {
        ...state,
        browseStatus: { type: "loading" },
        // A refusal belongs to the page it happened on: without this it would
        // sit above every later search for the rest of the session.
        visibilityStatus:
          state.visibilityStatus.type === "failed" ? { type: "idle" } : state.visibilityStatus,
      };
    case "vaultSearched":
      return {
        ...state,
        browse: event.payload.maps,
        browseQuery: event.payload.query,
        browseTotalPages: event.payload.totalPages,
        browseTotalRecords: event.payload.totalRecords,
        browseStatus: { type: "ready" },
      };
    case "vaultSearchFailed":
      return {
        ...state,
        browseStatus: { type: "failed", payload: { reason: event.payload.reason } },
      };
    case "installedLoading":
      return { ...state, installedStatus: { type: "loading" } };
    case "installedLoaded":
      return { ...state, installed: event.payload.maps, installedStatus: { type: "ready" } };
    case "installedLoadFailed":
      return {
        ...state,
        installedStatus: { type: "failed", payload: { reason: event.payload.reason } },
      };
    // Merge per size: a later read must not drop art an earlier one paid for.
    // An entry with both sides null is the "looked, found nothing" marker that
    // stops the image error handler asking again.
    case "localPreviewsLoaded": {
      const localPreviews = { ...state.localPreviews };
      const localPreviewOrder = [...state.localPreviewOrder];
      // Sorted, because the twin reads a `BTreeMap` and the eviction queue has
      // to end up in the same order on both sides. Object key order would
      // agree for every real folder name and quietly disagree for a numeric
      // one, which JavaScript hoists to the front.
      for (const folder of Object.keys(event.payload.previews).sort()) {
        const preview = event.payload.previews[folder];
        const known = localPreviews[folder];
        localPreviews[folder] = {
          small: preview.small ?? known?.small ?? null,
          large: preview.large ?? known?.large ?? null,
        };
        if (!known) localPreviewOrder.push(folder);
      }
      // Oldest first, so what the user is looking at now survives. See
      // `MAX_LOCAL_PREVIEWS`: this art is held as `data:` URLs and an
      // unbounded cache of it reached tens of megabytes on each side.
      while (Object.keys(localPreviews).length > MAX_LOCAL_PREVIEWS && localPreviewOrder.length > 0) {
        const evicted = localPreviewOrder.shift();
        if (evicted !== undefined) delete localPreviews[evicted];
      }
      return { ...state, localPreviews, localPreviewOrder };
    }
    case "matchmakerPoolsLoading":
      return { ...state, matchmakerPoolsStatus: { type: "loading" } };
    case "matchmakerPoolsLoaded":
      return {
        ...state,
        matchmakerPools: { ...state.matchmakerPools, [event.payload.queueName]: event.payload.pools },
        matchmakerPoolsStatus: { type: "ready" },
      };
    case "matchmakerPoolsLoadFailed":
      return {
        ...state,
        matchmakerPoolsStatus: { type: "failed", payload: { reason: event.payload.reason } },
      };
    case "installing":
      return {
        ...state,
        installStatus: { type: "installing", payload: { folderName: event.payload.folderName } },
      };
    // A map that had no art a moment ago may have some now, and the empty
    // "looked, found nothing" markers would otherwise outlive the folder they
    // describe.
    case "installed":
      return {
        ...state,
        installed: event.payload.installed,
        installedStatus: { type: "ready" },
        installStatus: { type: "idle" },
        localPreviews: {},
        localPreviewOrder: [],
      };
    case "installFailed":
      return { ...state, installStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    case "uninstalled":
      return {
        ...state,
        installed: event.payload.installed,
        installedStatus: { type: "ready" },
        installStatus: { type: "idle" },
        localPreviews: {},
        localPreviewOrder: [],
      };
    case "uninstallFailed":
      return { ...state, installStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    case "mapVisibilityChanging":
      return {
        ...state,
        visibilityStatus: { type: "working", payload: { versionId: event.payload.versionId } },
      };
    // Corrected in place rather than refetched: a refetch would move the page
    // under the user, and a freshly hidden version leaves every view but
    // "my maps". Both lists, because `vault` is the folder-name index nine
    // other features read and a stale flag there outlives this page.
    case "mapVisibilityChanged": {
      const { versionId, hidden } = event.payload;
      const mark = (map: MapsState["browse"][number]) =>
        map.versionId === versionId ? { ...map, hidden } : map;
      return {
        ...state,
        browse: state.browse.map(mark),
        vault: state.vault.map(mark),
        visibilityStatus: { type: "idle" },
      };
    }
    case "mapVisibilityFailed":
      return {
        ...state,
        visibilityStatus: { type: "failed", payload: { reason: event.payload.reason } },
      };
  }
}
