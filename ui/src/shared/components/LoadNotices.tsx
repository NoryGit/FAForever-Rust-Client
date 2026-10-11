// The two shapes of state a list has to be able to say out loud.
//
// `LoadStatusNotice` is for a fetch that the view depends on (a catalogue, a
// search, a scan): an error with Retry when it fails, nothing otherwise.
// While it runs, the status bar says so, in the one place the client reports
// what it is busy with (see `ClientStatusBar`), so this stays silent rather
// than saying it a second time somewhere else. `FailureNotice` is for an
// action that failed (an install, a toggle): the store keeps the failure until
// the next attempt, so the reader can dismiss the line once read and a new
// failure brings it back.

import { useState } from "react";
import { StatusNotice } from "../../design-system/StatusNotice";
import { useTranslation } from "../../i18n/useTranslation";
import type { LoadStatus } from "../loadStatusNote";
import { plainError } from "../plainError";

export function LoadStatusNotice({
  status,
  failed,
  onRetry,
}: {
  status: LoadStatus;
  /** What failed, without the reason, e.g. "Could not search the map vault". */
  failed: string;
  onRetry?: () => void;
}) {
  const { t } = useTranslation();
  if (status.type === "cancelled") {
    return (
      <StatusNotice tone="info" action={onRetry ? { label: t("common.retry"), onClick: onRetry } : undefined}>
        {t("status.activity.cancelled")}
      </StatusNotice>
    );
  }
  if (status.type !== "failed") return null;
  return (
    <StatusNotice
      tone="error"
      action={onRetry ? { label: t("common.retry"), onClick: onRetry } : undefined}
      detail={status.payload.reason}
    >
      {failed}: {plainError(status.payload.reason)}
    </StatusNotice>
  );
}

/** The reason a failed status carries, for the notice's tooltip. */
function reasonOf(status: { type: string }): string | undefined {
  if (!("payload" in status)) return undefined;
  const payload = status.payload;
  if (typeof payload !== "object" || payload === null || !("reason" in payload)) return undefined;
  return typeof payload.reason === "string" ? payload.reason : undefined;
}

export function FailureNotice({
  status,
  message,
}: {
  /** The store's status object; a new failure is a new object. */
  status: { type: string };
  /**
   * The sentence to show while `status` is a failure, or null otherwise. Build
   * it with `plainError`; the raw reason becomes the tooltip on its own.
   */
  message: string | null;
}) {
  const { t } = useTranslation();
  // The failure the reader dismissed, by identity: the same object stays in
  // the store until the next attempt, and the next failure is a new one.
  const [dismissed, setDismissed] = useState<object | null>(null);
  if (status.type !== "failed" || !message || dismissed === status) return null;
  return (
    <StatusNotice
      tone="error"
      secondary={{ label: t("common.dismiss"), onClick: () => setDismissed(status) }}
      detail={reasonOf(status)}
    >
      {message}
    </StatusNotice>
  );
}
