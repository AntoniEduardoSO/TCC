import os
import csv
import threading

class ScrapingState:
    def __init__(self):
        self.rows = []
        self.lock = threading.Lock()

    def load_csv(self, filepath):
        if not os.path.exists(filepath):
            return
        try:
            with open(filepath, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.rows = list(reader)
            print(f"Estado carregado: {len(self.rows)} registros.")
        except Exception as e:
            print(f"Erro ao ler CSV: {e}")

    def add(self, municipio, ano, periodo, status, portal_type, motivo="", detalhe=""):
        row = {
            "municipio": str(municipio),
            "ano": str(ano),
            "periodo": str(periodo),
            "status": status,
            "motivo": motivo,
            "detalhe": detalhe,
            "portal": portal_type
        }
        with self.lock: 
            self.rows.append(row)

    def is_ok(self, municipio, ano, periodo):
        municipio_str = str(municipio)
        ano_str = str(ano)
        periodo_str = str(periodo)

        with self.lock:
            for row in self.rows:
                if (
                    row["municipio"] == municipio_str and
                    row["ano"] == ano_str and
                    row["periodo"] == periodo_str and
                    row["status"] == "OK"
                ):
                    return True
        return False

    def save_csv(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if not self.rows:
            return

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=self.rows[0].keys()
            )
            writer.writeheader()
            writer.writerows(self.rows)

    def __len__(self):
        return len(self.rows)