// @vitest-environment happy-dom
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { applyEvent, initialState, seedStore } from "../testing/mounted";
import { useLinkRequests } from "./deepLinks";

const bridge = vi.hoisted(() => ({
  settle: vi.fn(),
  run: vi.fn((action: Promise<unknown>) => { void action.catch(() => undefined); }),
}));
vi.mock("../ipc/client", () => ({ ipc: bridge }));

const request = { id: 7, link: { type: "replay" as const, payload: { id: 42 } } };
const section = { kind: "Nav", command: { type: "selectReplaysSection", payload: { section: "online" } } };
const select = { kind: "Nav", command: { type: "select", payload: { tab: "replays" } } };
const acknowledge = { kind: "Nav", command: { type: "linkShown", payload: { id: request.id } } };

beforeEach(() => {
  vi.useFakeTimers();
  bridge.settle.mockReset().mockResolvedValue(undefined);
  seedStore((state) => ({ ...state, nav: { ...state.nav, linkRequest: request } }));
});
afterEach(() => vi.useRealTimers());

it("retries refused navigation and acknowledges only after it succeeds", async () => {
  bridge.settle.mockRejectedValueOnce(new Error("simulated IPC refusal"));
  renderHook(useLinkRequests);
  await act(async () => {});
  expect(bridge.settle.mock.calls).toEqual([[section]]);

  // An authoritative navigation update can carry a fresh copy of the request.
  applyEvent({ kind: "Nav", event: { ...initialState().nav, linkRequest: structuredClone(request) } });
  expect(bridge.settle).toHaveBeenCalledOnce();

  await act(() => vi.advanceTimersByTimeAsync(250));
  expect(bridge.settle.mock.calls).toEqual([[section], [section], [select], [acknowledge]]);
});

it("retries a refused acknowledgement without opening the link twice", async () => {
  bridge.settle
    .mockResolvedValueOnce(undefined)
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error("simulated acknowledgement refusal"));
  renderHook(useLinkRequests);
  await act(async () => {});
  expect(bridge.settle.mock.calls).toEqual([[section], [select], [acknowledge]]);

  await act(() => vi.advanceTimersByTimeAsync(250));
  expect(bridge.settle.mock.calls).toEqual([[section], [select], [acknowledge], [acknowledge]]);
});
