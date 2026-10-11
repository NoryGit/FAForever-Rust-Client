/** Common presentation for list-like backend states. */
export type LoadStatus =
  | { type: "idle" | "ready" | "loading" | "cancelled" }
  | { type: "failed"; payload: { reason: string } };

export function loadStatusNote(
  status: LoadStatus,
  loadingLabel: string,
  failedPrefix: string,
): string | null {
  switch (status.type) {
    case "idle":
    case "ready":
    case "cancelled":
      return null;
    case "loading":
      return loadingLabel;
    case "failed":
      return `${failedPrefix}: ${status.payload.reason}`;
  }
}
