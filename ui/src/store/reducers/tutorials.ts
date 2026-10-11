import type { TutorialsEvent, TutorialsState } from "../../ipc/bindings";

export function reduceTutorials(state: TutorialsState, event: TutorialsEvent): TutorialsState {
  switch (event.type) {
    case "launchCancelled":
      return { ...state, launch: { type: "idle" } };
    case "loading":
      return { ...state, status: { type: "loading" } };
    case "loaded": {
      // Twin of `faf_domain::state::tutorials::reduce`: keep the open lesson
      // across a refresh, but never point at one that has gone.
      const { categories, tutorials } = event.payload;
      const stillPresent = tutorials.some((t) => t.id === state.selectedId);
      return {
        ...state,
        categories,
        tutorials,
        status: { type: "ready" },
        selectedId: stillPresent ? state.selectedId : (tutorials[0]?.id ?? null),
      };
    }
    case "loadFailed":
      return { ...state, status: { type: "failed", payload: { reason: event.payload.reason } } };
    case "selected":
      return { ...state, selectedId: event.payload.tutorialId };
    case "launchPreparing":
      return {
        ...state,
        launch: {
          type: "preparing",
          payload: { tutorialId: event.payload.tutorialId, detail: event.payload.detail },
        },
      };
    case "launched":
      return {
        ...state,
        launch: { type: "launched", payload: { tutorialId: event.payload.tutorialId } },
      };
    case "launchFailed":
      return {
        ...state,
        launch: {
          type: "failed",
          payload: { tutorialId: event.payload.tutorialId, reason: event.payload.reason },
        },
      };
  }
}
