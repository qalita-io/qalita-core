"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Canal de sortie `figures.json` : agrégats sémantiquement décrits.

`metrics.json` porte les chiffres. `figures.json` porte ce qui les explique.
Un pack déclare une intention ; il ne choisit jamais le rendu.
"""

import sys

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
    if hasattr(frame, "collect"):  # LazyFrame polars, issu de scan_data()
        raise TypeError(
            "frame est un plan différé (LazyFrame) : appelez .collect() "
            '(ou .collect(engine="streaming")) sur votre agrégat avant de le '
            "passer — figures.py ne matérialise pas votre plan à votre place"
        )
    raise TypeError(
        "frame doit être un DataFrame pandas/polars ou une liste de dicts"
    )


def _limit(frame, size):
    """Restreint le frame à `size` lignes AVANT toute matérialisation."""
    if isinstance(frame, list):
        return frame[:size]
    if hasattr(frame, "head"):
        return frame.head(size)
    return frame


def _columns_of(frame, records):
    """Colonnes du frame : depuis les lignes, sinon depuis le schéma s'il est vide."""
    if records:
        return list(records[0].keys())
    if hasattr(frame, "columns"):  # DataFrame pandas/polars vide
        return list(frame.columns)
    return None  # liste de dicts vide : aucune colonne déclarée


def _to_records(frame, columns):
    """Extrait `columns` en listes positionnelles, dans l'ordre demandé."""
    records = _records_of(frame)
    available = _columns_of(frame, records)

    if available is not None:
        for col in columns:
            if col not in available:
                raise ValueError(f"Colonne '{col}' absente du frame")

    return [[record.get(col) for col in columns] for record in records]


def _take(frame, records, positions, folded, columns):
    """Reconstruit le frame d'origine restreint à `positions`, plus la ligne `folded`.

    Les lignes conservées sont prélevées sur le frame d'origine, jamais
    reconstruites à partir de dicts : le type de chaque colonne est celui de
    l'entrée. Seule la ligne repliée peut élargir un type, et uniquement sur
    les colonnes qu'elle renseigne.
    """
    if isinstance(frame, list):
        rows = [records[i] for i in positions]
        return rows + [folded] if folded is not None else rows

    # pandas — un frame polars ne passe jamais par ici, il est trié dans le
    # moteur par `_top_n_polars`.
    import pandas as pd

    head = frame.iloc[positions].reset_index(drop=True)
    if folded is None:
        return head
    fold_frame = type(frame)([folded], columns=columns)
    for column in columns:
        # Une colonne repliée à None est toute-NA : sans ce recalage de type,
        # pandas re-déduit le type de la colonne entière à la concaténation.
        if folded[column] is None:
            try:
                fold_frame[column] = fold_frame[column].astype(
                    head[column].dtype
                )
            except (TypeError, ValueError):
                pass
    return pd.concat([head, fold_frame], ignore_index=True)


def _reject_duplicate_dim_tuples(key, rows, dim_count):
    """Un agrégat a une ligne par tuple de dimensions ; un doublon signe des lignes brutes."""
    seen = set()
    for row in rows:
        # repr : identité fiable pour des scalaires homogènes, et jamais
        # d'exception sur une valeur non hachable.
        signature = tuple(repr(value) for value in row[:dim_count])
        if signature in seen:
            raise ValueError(
                f"figure '{key}' : le tuple de dimensions "
                f"{tuple(row[:dim_count])} apparaît plusieurs fois — `frame` "
                "doit être un agrégat (une ligne par tuple de dimensions), "
                "pas les lignes brutes de la source"
            )
        seen.add(signature)


def _fold_target(columns, by, dim, label):
    """Colonne qui reçoit `label` sur la ligne repliée.

    Nommée par `dim`, ou déduite quand une seule colonne peut la porter. Jamais
    devinée par position : rien ne contraint l'ordre des colonnes d'un frame, et
    une dimension placée après une mesure ferait atterrir le label dans cette
    mesure — exactement la corruption que la ligne repliée doit éviter.
    """
    if dim is not None:
        if dim == by:
            raise ValueError(
                f"top_n : `dim` ne peut pas être '{by}', la mesure repliée — "
                "`dim` nomme la dimension qui porte le label"
            )
        return dim

    candidates = [c for c in columns if c != by]
    if not candidates:
        raise ValueError(
            f"top_n : aucune colonne ne peut porter le label '{label}' — le "
            f"frame n'a que la colonne '{by}', et la ligne repliée serait "
            "indiscernable d'une vraie ligne"
        )
    if len(candidates) > 1:
        raise ValueError(
            f"top_n : plusieurs colonnes hors '{by}' "
            f"({', '.join(str(c) for c in candidates)}) — précisez dim= pour "
            "nommer celle qui porte le label ; la déduire de sa position "
            "écrirait le label dans une mesure"
        )
    return candidates[0]


def _is_polars_frame(frame):
    """Vrai pour un `pl.DataFrame` — sans importer polars s'il n'est pas là."""
    polars = sys.modules.get("polars")
    return polars is not None and isinstance(frame, polars.DataFrame)


def _top_n_polars(frame, by, n, other, label, dimension):
    """Version polars de `top_n` : le tri et le repli restent dans le moteur.

    `top_n` est nourri de résultats de `group_by` — le cas à forte cardinalité
    par excellence. La version générique matérialise tout le frame en dicts
    Python puis trie en Python ; ici seules les `n` lignes gardées (plus la
    ligne repliée) traversent la frontière Python.
    """
    import polars as pl

    from qalita_core import analytics

    head = analytics.top_k(frame, by, n) if n > 0 else frame.head(0)
    if not other or frame.height <= n:
        return head

    tail = analytics.agg(
        frame.lazy().sort(by, descending=True, nulls_last=True).slice(n),
        {"total": pl.col(by).sum()},
    )
    folded = {column: None for column in frame.columns}
    folded[dimension] = label
    folded[by] = tail["total"] or 0
    # vertical_relaxed : une colonne nulle prend le type de la tête, et la
    # colonne repliée prend le supertype (str) sans toucher aux autres.
    return pl.concat([head, type(frame)([folded])], how="vertical_relaxed")


def top_n(frame, by, n, other=False, label="Autres", dim=None):
    """Garde les n plus grandes lignes selon `by`, en jetant la queue.

    `other=True` replie la queue en une ligne `label` au lieu de la jeter. À
    n'utiliser que sur une mesure additive (un compte) : replier des ratios en
    les sommant produit un chiffre faux — dans ce cas, laisser la troncature
    faire son travail et afficher la note de dépassement.

    `dim` nomme la colonne qui porte `label` sur la ligne repliée. Elle n'est
    déduite que si une seule colonne hors `by` existe ; au-delà, `dim` est
    exigé plutôt que deviné.

    La ligne repliée ne fabrique aucune valeur : `label` va dans `dim`, `by`
    porte la somme de la queue, et toute autre colonne est nulle.

    Sur un `pl.DataFrame`, le classement est délégué au moteur (voir
    `_top_n_polars`) : seules les lignes conservées deviennent des objets
    Python. Un `LazyFrame` reste refusé — agrégez et collectez d'abord.
    """
    polars_frame = _is_polars_frame(frame)
    # Un frame polars n'est jamais matérialisé en dicts : ses colonnes se lisent
    # sur le schéma, et le tri reste dans le moteur.
    records = None if polars_frame else _records_of(frame)
    columns = (
        list(frame.columns) if polars_frame else _columns_of(frame, records)
    )

    if columns is not None:
        if by not in columns:
            raise ValueError(f"Colonne '{by}' absente du frame")
        if dim is not None and dim not in columns:
            raise ValueError(f"Colonne '{dim}' absente du frame")

    dimension = None
    if other and columns is not None:
        # Résolu même sans queue à replier : le contrat d'appel ne doit pas
        # dépendre du contenu du frame du jour.
        dimension = _fold_target(columns, by, dim, label)

    if polars_frame:
        return _top_n_polars(frame, by, n, other, label, dimension)

    positions = sorted(
        range(len(records)),
        key=lambda i: records[i].get(by) or 0,
        reverse=True,
    )
    head_positions, tail_positions = positions[:n], positions[n:]

    folded = None
    if other and tail_positions:
        folded = {column: None for column in columns}
        folded[dimension] = label
        folded[by] = sum((records[i].get(by) or 0) for i in tail_positions)

    return _take(frame, records, head_positions, folded, columns)


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
        """Ajoute une figure. `dims` accepte "nom" (nominal par défaut) ou ("nom", "temporal").

        `frame` doit être un **agrégat** : au moins une dimension, au moins une
        mesure, et exactement une ligne par tuple de dimensions. Une figure
        explique un chiffre, elle ne transporte pas les lignes de la source —
        y déverser des lignes brutes ferait sortir des données individuelles
        de chez le client.

        Ces trois règles sont vérifiées ici, mais **ne couvrent pas tout** : des
        lignes brutes clés par un identifiant unique n'ont, par construction,
        aucun tuple de dimensions en double. `dims=["patient_id"],
        measures=["age"]` passe les trois garde-fous et expédie 5000 lignes
        patient marquées `truncated: true`. Aucun contrôle automatique ne
        remplace la règle : ne passez à `add` qu'un frame que vous avez
        vous-même agrégé.
        """
        # Lier une fois pour toutes : `dims` et `measures` sont re-parcourus
        # plusieurs fois plus bas, et un itérateur à usage unique serait
        # consommé par le premier garde-fou.
        dims = list(dims)
        measures = list(measures)

        if intent not in INTENTS:
            raise ValueError(
                f"intent '{intent}' inconnu — attendus : {', '.join(INTENTS)}"
            )

        if not dims:
            raise ValueError(
                f"figure '{key}' : `dims` est vide — une figure décrit un "
                "agrégat, qui a au moins une dimension"
            )

        if not measures:
            raise ValueError(
                f"figure '{key}' : `measures` est vide — une figure décrit un "
                "agrégat, qui a au moins une mesure ; sans mesure, `add` "
                "n'est plus qu'une projection de colonnes brutes"
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

        columns = [d["name"] for d in normalized_dims] + measures
        # Une ligne de plus que le plafond suffit à savoir qu'il est dépassé :
        # un frame de 50M de lignes passé par erreur ne doit pas faire tomber
        # le worker avant même d'être marqué tronqué.
        rows = _to_records(_limit(frame, max_rows + 1), columns)

        # Assainir AVANT le contrôle de doublons : deux valeurs distinctes en
        # entrée (None et NaN, np.int64(1) et 1) deviennent une seule valeur
        # expédiée, et le contrôle doit voir ce qui part, pas ce qui entre.
        rows = _sanitize_for_json(rows)
        _reject_duplicate_dim_tuples(key, rows, len(normalized_dims))

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
                "rows": rows,
                "truncated": truncated,
            }
        )

    def save(self):
        """Vérifie que chaque figure référence des mesures déclarées, puis écrit."""
        declared = set(self.data["measures"])
        for figure in self.data["figures"]:
            names = list(figure.get("measures") or [])
            if figure.get("of") is not None:
                names.append(figure["of"])
            for name in names:
                if name not in declared:
                    raise ValueError(
                        f"figure '{figure['key']}' : mesure '{name}' non "
                        "déclarée — appelez declare_measure('"
                        f"{name}') ; sinon la figure ne sera jamais reliée à "
                        "son chiffre"
                    )
        super().save()

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
