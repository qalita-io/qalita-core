"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Canal de sortie `figures.json` : agrégats sémantiquement décrits.

`metrics.json` porte les chiffres. `figures.json` porte ce qui les explique.
Un pack déclare une intention ; il ne choisit jamais le rendu.
"""

from qalita_core.pack import PlatformAsset, _sanitize_for_json

INTENTS = (
    "breakdown",
    "composition",
    "distribution",
    "trend",
    "comparison",
    "matrix",
    "flow",
)

DIM_TYPES = ("nominal", "ordinal", "temporal")

MAX_ROWS = 5000


def _records_of(frame):
    """Normalise un DataFrame pandas/polars ou une liste de dicts en liste de dicts."""
    if hasattr(frame, "to_dicts"):  # polars
        return frame.to_dicts()
    if hasattr(frame, "to_dict"):  # pandas
        return frame.to_dict(orient="records")
    if isinstance(frame, list):
        return list(frame)
    raise TypeError(
        "frame doit être un DataFrame pandas/polars ou une liste de dicts"
    )


def _to_records(frame, columns):
    """Extrait `columns` en listes positionnelles, dans l'ordre demandé."""
    records = _records_of(frame)

    if records:
        available = set(records[0].keys())
    elif hasattr(
        frame, "columns"
    ):  # DataFrame pandas/polars vide : colonnes connues même sans lignes
        available = set(frame.columns)
    else:  # liste de dicts vide : aucune colonne déclarée à vérifier
        available = None

    if available is not None:
        for col in columns:
            if col not in available:
                raise ValueError(f"Colonne '{col}' absente du frame")

    return [[record.get(col) for col in columns] for record in records]


def top_n(frame, by, n, other=True, label="Autres"):
    """Garde les n plus grandes lignes selon `by`, replie la queue en une ligne `label`.

    À n'utiliser que sur une mesure additive (un compte). Replier des ratios en
    les sommant produit un chiffre faux : dans ce cas, laisser la troncature
    faire son travail et afficher la note de dépassement.
    """
    records = _records_of(frame)
    rebuild = list if isinstance(frame, list) else type(frame)

    records = sorted(records, key=lambda r: r.get(by) or 0, reverse=True)
    head, tail = records[:n], records[n:]

    if other and tail:
        key_columns = [c for c in records[0].keys() if c != by]
        folded = {c: label for c in key_columns}
        folded[by] = sum((r.get(by) or 0) for r in tail)
        head = head + [folded]

    return head if rebuild is list else rebuild(head)


class FiguresAsset(PlatformAsset):
    """Assemble le contrat figures.json v1."""

    def __init__(self):
        super().__init__("figures")
        self.data = {"version": 1, "measures": {}, "figures": []}

    def declare_measure(
        self,
        key,
        *,
        unit=None,
        direction=None,
        target=None,
        warn=None,
        label=None,
        description=None,
    ):
        """Déclare la sémantique d'une mesure. Référençable par une figure ET par une clé de metrics.json."""
        semantics = {
            "unit": unit,
            "direction": direction,
            "target": target,
            "warn": warn,
            "label": label,
            "description": description,
        }
        self.data["measures"][key] = {
            k: v for k, v in semantics.items() if v is not None
        }

    def add(
        self,
        key,
        *,
        intent,
        frame,
        dims,
        measures,
        scope,
        of=None,
        title=None,
        max_rows=MAX_ROWS,
    ):
        """Ajoute une figure. `dims` accepte "nom" (nominal par défaut) ou ("nom", "temporal")."""
        if intent not in INTENTS:
            raise ValueError(
                f"intent '{intent}' inconnu — attendus : {', '.join(INTENTS)}"
            )

        normalized_dims = []
        for dim in dims:
            name, dim_type = (
                dim if isinstance(dim, tuple) else (dim, "nominal")
            )
            if dim_type not in DIM_TYPES:
                raise ValueError(
                    f"type de dimension '{dim_type}' inconnu — attendus : {', '.join(DIM_TYPES)}"
                )
            normalized_dims.append({"name": name, "type": dim_type})

        columns = [d["name"] for d in normalized_dims] + list(measures)
        rows = _to_records(frame, columns)

        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]

        self.data["figures"].append(
            {
                "key": key,
                "intent": intent,
                "of": of,
                "scope": scope,
                "title": title,
                "dims": normalized_dims,
                "measures": list(measures),
                "rows": _sanitize_for_json(rows),
                "truncated": truncated,
            }
        )

    def add_raw(self, key, *, option, scope, title=None):
        """Échappatoire : option ECharts brute. Exclue du self-service et du reporting."""
        self.data["figures"].append(
            {
                "key": key,
                "intent": "raw",
                "of": None,
                "scope": scope,
                "title": title,
                "option": _sanitize_for_json(option),
                "truncated": False,
            }
        )
