from __future__ import annotations

from pathlib import Path
from typing import Any

import requests
from textual.app import App, ComposeResult
from textual.message import Message
from textual.widgets import Footer, Header, Label, ListItem, ListView, TextArea


class ExperimentInfo(TextArea):
    pass


class ExperimentEntry(ListItem):
    def __init__(self, experiment: dict[str, str]) -> None:
        super().__init__()
        self.experiment = experiment

    def compose(self) -> ComposeResult:
        yield Label(self.experiment["id"])


class ExperimentBrowser(ListView):
    def __init__(
        self, server_address: Path | None = None, *args: Any, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._server_address = (
            server_address if server_address is not None else "http://127.0.0.1:8000/"
        )

    class ExperimentChanged(Message):
        def __init__(self, id: str) -> None:
            super().__init__()
            self.id = id

    def on_mount(self) -> None:
        self._refresh()

    def _refresh(self) -> None:
        # Clear out anything that's in here right now.
        self.clear()
        # Now populate with the content of the current working directory. We
        # want to be able to go up, so let's make sure there's an entry for
        response = requests.get(self._server_address + "experiments")
        if response.status_code == 200:
            items = response.json()
            for experiment in items:
                self.append(ExperimentEntry(experiment))

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        # If the user selected a directory entry...
        event.stop()
        msg = self.experiment_info(event.item.experiment.get("id"))
        txt = ""
        for v in msg.values():
            txt += str(v) + ","
        self.post_message(self.ExperimentChanged(id=txt))

    def experiment_info(self, exp_id: str):
        print("GET EXP INFO")
        response = requests.get(
            self._server_address + "experiments/" + exp_id + "/state"
        )
        if response.status_code == 200:
            items = response.json()
            return items


class ExperimentBrowserApp(App[None]):
    CSS_PATH = "experiment_view.tcss"

    def compose(self) -> ComposeResult:
        yield Header()
        yield ExperimentBrowser()
        yield ExperimentInfo()
        yield Footer()

    def on_experiment_browser_experiment_changed(
        self, event: ExperimentBrowser.ExperimentChanged
    ) -> None:
        self.query_one(ExperimentInfo).clear()
        self.query_one(ExperimentInfo).insert(event.id)


if __name__ == "__main__":
    ExperimentBrowserApp().run()
