from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re

from bs4 import BeautifulSoup
from pydantic import JsonValue

from .sources_v1 import ParserIncompatibleError


PARSER_ID = "inss_employee_table_v1"
PARSER_VERSION = "1.1.0"
REFERENCE_YEAR = 2026
CANONICAL_SOURCE_ID = "INSS_TABLE_2026"


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def _br_money(value: str) -> str:
    raw = re.sub(r"[^0-9,.-]", "", value or "")
    raw = raw.replace(".", "").replace(",", ".")
    try:
        amount = Decimal(raw)
    except InvalidOperation as exc:
        raise ParserIncompatibleError(f"invalid BRL amount: {value!r}") from exc
    return format(amount.quantize(Decimal("0.01")), "f")


def _rate(value: str) -> str:
    raw = re.sub(r"[^0-9,.-]", "", value or "").replace(",", ".")
    try:
        pct = Decimal(raw)
    except InvalidOperation as exc:
        raise ParserIncompatibleError(f"invalid percentage: {value!r}") from exc
    return format((pct / Decimal("100")).normalize(), "f")


def _assert_contiguous(bands: list[dict[str, JsonValue]]) -> None:
    cent = Decimal("0.01")
    for previous, current in zip(bands, bands[1:]):
        previous_upper = previous.get("upper_bound_brl")
        current_lower = current.get("lower_bound_brl")
        if not isinstance(previous_upper, str) or not isinstance(current_lower, str):
            raise ParserIncompatibleError("INSS employee table lost bounded progressive bands")
        if Decimal(current_lower) != Decimal(previous_upper) + cent:
            raise ParserIncompatibleError("INSS employee progressive bands are not contiguous")


def parse_inss_employee_snapshot(body: bytes) -> dict[str, JsonValue]:
    """Normalize the current canonical INSS employee contribution table.

    The parser is deliberately bound to the registered operational table page.
    It does not discover annual news articles and does not accept them as a
    fallback input. A source-layout or semantic-marker change fails closed as
    PARSER_INCOMPATIBLE so Phase 5 can decide what changed.
    """

    try:
        html = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParserIncompatibleError("INSS snapshot is not valid UTF-8") from exc

    soup = BeautifulSoup(html, "html.parser")
    text = _normalize_space(soup.get_text(" ", strip=True))
    folded = text.casefold()

    required_markers = (
        "tabela de contribuição mensal",
        "1. para empregado, empregado doméstico e trabalhador avulso",
        "2. para contribuinte individual, facultativo e microempreendedor individual",
    )
    missing = [marker for marker in required_markers if marker.casefold() not in folded]
    if missing:
        raise ParserIncompatibleError(f"INSS canonical structural markers missing: {missing}")

    year_match = re.search(
        r"tabelas válidas a partir da competência janeiro de\s+(20\d{2})",
        folded,
        re.IGNORECASE,
    )
    if not year_match:
        raise ParserIncompatibleError("INSS reference-year marker changed")
    reference_year = int(year_match.group(1))

    employee_section = re.search(
        r"1\.\s*Para Empregado, Empregado Doméstico e Trabalhador Avulso:\s*(.*?)\s*"
        r"2\.\s*Para Contribuinte Individual, Facultativo e Microempreendedor Individual",
        text,
        re.IGNORECASE,
    )
    if not employee_section:
        raise ParserIncompatibleError("INSS employee section boundaries changed")

    section = employee_section.group(1)
    first = re.search(
        r"Até\s*R\$\s*([\d\.,]+)\s*([\d\.,]+)%",
        section,
        re.IGNORECASE,
    )
    middle = list(
        re.finditer(
            r"De\s*R\$\s*([\d\.,]+)\s*(?:a|até)\s*R\$\s*([\d\.,]+)\s*([\d\.,]+)%",
            section,
            re.IGNORECASE,
        )
    )
    if not first or len(middle) != 3:
        raise ParserIncompatibleError("INSS employee progressive table shape changed")

    bands: list[dict[str, JsonValue]] = [
        {
            "lower_bound_brl": None,
            "upper_bound_brl": _br_money(first.group(1)),
            "rate": _rate(first.group(2)),
        }
    ]
    for match in middle:
        bands.append(
            {
                "lower_bound_brl": _br_money(match.group(1)),
                "upper_bound_brl": _br_money(match.group(2)),
                "rate": _rate(match.group(3)),
            }
        )

    _assert_contiguous(bands)

    normative = re.search(
        r"PORTARIA\s+INTERMINISTERIAL\s+MPS/MF\s+N[º°]?\s*13\s*,\s*de\s*09/01/2026",
        text,
        re.IGNORECASE,
    )
    if not normative:
        raise ParserIncompatibleError("INSS 2026 normative-reference marker changed")

    separate_thirteenth = re.search(
        r"décimo terceiro salário.*?não deve ser somado à remuneração mensal.*?valores em separado",
        text,
        re.IGNORECASE,
    )
    if not separate_thirteenth:
        raise ParserIncompatibleError("INSS 13th separate-assessment marker changed")

    ceiling = bands[-1]["upper_bound_brl"]
    if not isinstance(ceiling, str):
        raise ParserIncompatibleError("INSS employee contribution ceiling is missing")

    return {
        "observation_type": f"inss_employee_progressive_table_{reference_year}",
        "reference_year": reference_year,
        "effective_from": f"{reference_year}-01-01",
        "employment_categories": [
            "empregado",
            "empregado_domestico",
            "trabalhador_avulso",
        ],
        "calculation_method": "marginal_by_bracket",
        "monthly_table": bands,
        "contribution_ceiling_brl": ceiling,
        "thirteenth_assessment": "separate_from_monthly_remuneration",
        "normative_reference": {
            "act_id": (
                f"PORTARIA_INTERMINISTERIAL_MPS_MF_{normative.group(1)}_{reference_year}"
            ),
            "act_date": (
                f"{reference_year}-{int(normative.group(3)):02d}-{int(normative.group(2)):02d}"
            ),
        },
    }


# Compatibility alias kept during the first evergreen migration layer.
# Historical tests/callers may still import the year-qualified symbol.
def parse_inss_employee_2026_snapshot(body: bytes) -> dict[str, JsonValue]:
    return parse_inss_employee_snapshot(body)
