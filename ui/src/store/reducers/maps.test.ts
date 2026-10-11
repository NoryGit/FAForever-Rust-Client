// Conformance tests for the frontend maps reducer, the twin of
// `faf_domain::state::maps::reduce`.
//
// The local-preview cache is the part worth pinning. It is written from two
// directions (a tile that ran out of remote candidates asks for one size, a
// detail pane for the other) and read from an image error handler, so both the
// merge and the "looked, found nothing" marker are load-bearing: get either
// wrong and the co-op panes either lose art they already have or re-read the
// maps folder on every render.

import { describe, expect, it } from "vitest";
import type { MapsEvent, MapsState, MapVaultQuery } from "../../ipc/bindings";
import { reduceMaps } from "./maps";

const EMPTY_QUERY = {
  search: "",
  sortBy: "newest",
  page: 1,
  pageSize: 20,
} as unknown as MapVaultQuery;

function state(overrides: Partial<MapsState> = {}): MapsState {
  return {
    vault: [],
    vaultStatus: { type: "idle" },
    vaultProgress: null,
    browse: [],
    browseStatus: { type: "idle" },
    browseQuery: EMPTY_QUERY,
    browseTotalPages: null,
    browseTotalRecords: null,
    installed: [],
    installedStatus: { type: "idle" },
    installStatus: { type: "idle" },
    visibilityStatus: { type: "idle" },
    matchmakerPools: {},
    matchmakerPoolsStatus: { type: "idle" },
    localPreviews: {},
    localPreviewOrder: [],
    ...overrides,
  };
}

function loaded(previews: MapsState["localPreviews"]): MapsEvent {
  return { type: "localPreviewsLoaded", payload: { previews } };
}

describe("local map previews", () => {
  it("keeps a size an earlier read already paid for", () => {
    const before = state({
      localPreviews: { scca_coop_a01: { small: "data:small", large: null } },
    });

    const after = reduceMaps(before, loaded({ scca_coop_a01: { small: null, large: "data:large" } }));

    expect(after.localPreviews.scca_coop_a01).toEqual({
      small: "data:small",
      large: "data:large",
    });
  });

  it("records a fruitless look so it is not repeated", () => {
    const after = reduceMaps(state(), loaded({ plain_map: { small: null, large: null } }));

    // Present, and empty: the caller checks for the key, not for a value.
    expect(after.localPreviews).toHaveProperty("plain_map");
    expect(after.localPreviews.plain_map).toEqual({ small: null, large: null });
  });

  it("leaves other maps alone", () => {
    const before = state({
      localPreviews: { other_map: { small: "data:other", large: null } },
    });

    const after = reduceMaps(before, loaded({ scca_coop_a01: { small: "data:new", large: null } }));

    expect(after.localPreviews.other_map).toEqual({ small: "data:other", large: null });
    expect(after.localPreviews.scca_coop_a01).toEqual({ small: "data:new", large: null });
  });

  // The art is held as `data:` URLs and mirrored into this process, so an
  // unbounded cache of it is tens of megabytes twice over on a real
  // installation. Twin of `the_preview_cache_evicts_its_oldest_entries`.
  it("evicts the oldest entries once the cache is full", () => {
    const key = (index: number) => `map_${String(index).padStart(4, "0")}`;
    let current = state();
    for (let index = 0; index < 140; index += 1) {
      current = reduceMaps(current, loaded({ [key(index)]: { small: "data:art", large: null } }));
    }

    expect(Object.keys(current.localPreviews)).toHaveLength(128);
    expect(current.localPreviewOrder).toHaveLength(128);
    // The twelve oldest are gone, the newest is not.
    expect(current.localPreviews[key(0)]).toBeUndefined();
    expect(current.localPreviews[key(11)]).toBeUndefined();
    expect(current.localPreviews[key(12)]).toBeDefined();
    expect(current.localPreviews[key(139)]).toBeDefined();
    expect(current.localPreviewOrder[0]).toBe(key(12));
  });

  it("does not queue a folder twice when a second size arrives", () => {
    let current = reduceMaps(state(), loaded({ one_map: { small: "data:small", large: null } }));
    current = reduceMaps(current, loaded({ one_map: { small: null, large: "data:large" } }));

    expect(current.localPreviewOrder).toEqual(["one_map"]);
  });

  it("empties the eviction queue with the cache when maps are installed", () => {
    const before = state({
      localPreviews: { one_map: { small: "data:small", large: null } },
      localPreviewOrder: ["one_map"],
    });

    const after = reduceMaps(before, { type: "installed", payload: { installed: [] } });

    expect(after.localPreviews).toEqual({});
    expect(after.localPreviewOrder).toEqual([]);
  });
});
