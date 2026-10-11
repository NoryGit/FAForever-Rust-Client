// @vitest-environment happy-dom

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { failOnConsoleError } from "../../testing/consoleGuard";
import { sentCommands } from "../../testing/mounted";
import { BackgroundActivityTask } from "./ClientStatusBar";

vi.mock("../../ipc/client");
failOnConsoleError();

it("cancels the named catalogue without cancelling another background task", async () => {
  const user = userEvent.setup();
  render(<BackgroundActivityTask activities={[
    { label: "Map catalogue: 2 / 5 pages", progress: 40, cancel: { kind: "Maps", command: { type: "cancelVaultLoad" } } },
    { label: "Mod catalogue", cancel: { kind: "Mods", command: { type: "cancelVaultLoad" } } },
  ]} />);
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("40");
  await user.click(screen.getByRole("button", { name: "Cancel: Map catalogue: 2 / 5 pages" }));
  expect(sentCommands()).toEqual([{ kind: "Maps", command: { type: "cancelVaultLoad" } }]);
});
