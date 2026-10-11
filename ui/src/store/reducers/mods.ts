import type { ModsEvent, ModsState } from "../../ipc/bindings";

export function reduceMods(state: ModsState, event: ModsEvent): ModsState {
  switch (event.type) {
    case "vaultProgress":
      return { ...state, vaultProgress: event.payload.progress };
    case "vaultCancelled":
      return { ...state, vaultStatus: { type: "cancelled" }, vaultProgress: null };
    case "vaultLoading":
      return { ...state, vaultStatus: { type: "loading" }, vaultProgress: null };
    case "vaultLoaded":
      return { ...state, vault: event.payload.mods, vaultStatus: { type: "ready" } };
    case "vaultLoadFailed":
      return { ...state, vaultStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    case "vaultSearching":
      return { ...state, browseStatus: { type: "loading" } };
    case "vaultSearched":
      return {
        ...state,
        browse: event.payload.mods,
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
      return { ...state, installed: event.payload.mods, installedStatus: { type: "ready" } };
    case "installedLoadFailed":
      return {
        ...state,
        installedStatus: { type: "failed", payload: { reason: event.payload.reason } },
      };
    case "installing":
      return {
        ...state,
        installStatus: { type: "installing", payload: { uid: event.payload.uid } },
      };
    case "installed":
      return {
        ...state,
        installed: event.payload.installed,
        installedStatus: { type: "ready" },
        installStatus: { type: "idle" },
      };
    case "installFailed":
      return { ...state, installStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    case "uninstalled":
      return {
        ...state,
        installed: event.payload.installed,
        installedStatus: { type: "ready" },
        installStatus: { type: "idle" },
      };
    case "uninstallFailed":
      return { ...state, installStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    case "toggling":
      return {
        ...state,
        toggleStatus: { type: "toggling", payload: { uid: event.payload.uid } },
      };
    case "toggled":
      return {
        ...state,
        installed: event.payload.installed,
        installedStatus: { type: "ready" },
        toggleStatus: { type: "idle" },
      };
    case "toggleFailed":
      return { ...state, toggleStatus: { type: "failed", payload: { reason: event.payload.reason } } };
    case "downloadSizesResolved": {
      // Only the answers that carried a number. A uid that is absent has not
      // been asked about or came back without a length, and both mean "do not
      // print a size", which is the same rule the Rust reducer applies.
      const sizes = { ...state.downloadSizes };
      for (const size of event.payload.sizes) {
        if (size.bytes !== null) sizes[size.uid] = size.bytes;
      }
      return { ...state, downloadSizes: sizes };
    }
  }
}
