import csv
import os

from datetime import datetime
from threading import Lock

STATE_FOLDER = os.path.join("data", "state")
os.makedirs(STATE_FOLDER, exist_ok=True)

LOCK = Lock()

STATUS_OK = "OK"
STATUS_EMPTY = "EMPTY"
STATUS_ERROR = "ERROR"
STATUS_INCONSISTENT = "INCONSISTENT"

class State:
    def __init__(self, portal_name):
        self.portal = portal_name
        self.path = os.path.join(STATE_FOLDER, f"{portal_name}.csv")
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow([
                    "portal",
                    "municipio",
                    "ibge",
                    "ano",
                    "status",
                    "linhas",
                    "mensagem",
                    "timestamp"
                ])
    
    def _read_all(self):
        if not os.path.exists(self.path):
            return []

        with open(self.path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            return list(reader)

    def is_done(self, municipio, ano):
        rows = self._read_all()
        for r in rows:
            if r["municipio"] == municipio and int(r["ano"]) == ano:
                return r["status"] == STATUS_OK or r["status"] == STATUS_EMPTY
        return False

    def should_retry(self, municipio, ano):
        rows = self._read_all()
        for r in rows:
            if r["municipio"] == municipio and int(r["ano"]) == ano:
                return r["status"] in (STATUS_ERROR, STATUS_INCONSISTENT)
        return True

    def _append(self, municipio, ibge, ano, status, linhas=0, msg=""):
        with LOCK:
            with open(self.path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow([
                    self.portal,
                    municipio,
                    ibge,
                    ano,
                    status,
                    linhas,
                    msg,
                    datetime.now().isoformat(timespec="seconds")
                ])

    def mark_ok(self, municipio, ibge, ano, linhas):
        self._append(municipio, ibge, ano, STATUS_OK, linhas)

    def mark_empty(self, municipio, ibge, ano):
        self._append(municipio, ibge, ano, STATUS_EMPTY, 0, "sem dados")

    def mark_error(self, municipio, ibge, ano, msg):
        self._append(municipio, ibge, ano, STATUS_ERROR, 0, msg)

    def mark_inconsistent(self, municipio, ibge, ano, msg):
        self._append(municipio, ibge, ano, STATUS_INCONSISTENT, 0, msg)