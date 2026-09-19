from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re

from bs4 import BeautifulSoup
from pydantic import JsonValue

from .source_ids_v1 import RFB_SOURCE_ID
from .sources_v1 import ParserIncompatibleError


PARSER_ID = "rfb_irrf_table_v1"
PARSER_VERSION = "1.1.0"
REFERENCE_YEAR = 2026


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


def _decimal(value: str) -> str:
    raw = re.sub(r"[^0-9,.-]", "", value or "").replace(",", ".")
    try:
        number = Decimal(raw)
    except InvalidOperation as exc:
        raise ParserIncompatibleError(f"invalid decimal: {value!r}") from exc
    return format(number.normalize(), "f")


def parse_rfb_irrf_snapshot(body: bytes) -> dict[str, JsonValue]:
    """Parse the current RFB annual IRRF page into a normalized source observation.

    This parser does not publish fiscal rules. It only normalizes values observed
    in the already-known official source structure so Phase 5 can compare them.
    """

    try:
        html = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParserIncompatibleError("RFB snapshot is not valid UTF-8") from exc

    soup = BeautifulSoup(html, "html.parser")
    text = _normalize_space(soup.get_text(" ", strip=True))

    required_markers = (
        "Tabela de Incidência Mensal",
        "Tabela de Redução Mensal",
    )
    missing = [marker for marker in required_markers if marker not in text]
    if missing:
        raise ParserIncompatibleError(f"RFB structural markers missing: {missing}")

    year_match = re.search(r"Tributação de\s+(20\d{2})", text, re.IGNORECASE)
    if not year_match:
        raise ParserIncompatibleError("RFB reference-year marker changed")
    reference_year = int(year_match.group(1))

    monthly_start = text.index("Tabela de Incidência Mensal")
    reduction_start = text.index("Tabela de Redução Mensal")
    if reduction_start <= monthly_start:
        raise ParserIncompatibleError("RFB section order changed")

    monthly = text[monthly_start:reduction_start]
    reduction = text[reduction_start:]

    first = re.search(r"Até R\$\s*([\d\.,]+)\s*-\s*-", monthly, re.IGNORECASE)
    middle = list(
        re.finditer(
            r"De R\$\s*([\d\.,]+)\s*até R\$\s*([\d\.,]+)\s*([\d\.,]+)%\s*R\$\s*([\d\.,]+)",
            monthly,
            re.IGNORECASE,
        )
    )
    top = re.search(
        r"Acima de R\$\s*([\d\.,]+)\s*([\d\.,]+)%\s*R\$\s*([\d\.,]+)",
        monthly,
        re.IGNORECASE,
    )
    if not first or len(middle) != 3 or not top:
        raise ParserIncompatibleError("RFB monthly IRRF table shape changed")

    bands: list[dict[str, JsonValue]] = [
        {
            "lower_bound_brl": None,
            "upper_bound_brl": _br_money(first.group(1)),
            "rate": "0",
            "deduction_brl": "0.00",
        }
    ]
    for match in middle:
        bands.append(
            {
                "lower_bound_brl": _br_money(match.group(1)),
                "upper_bound_brl": _br_money(match.group(2)),
                "rate": _rate(match.group(3)),
                "deduction_brl": _br_money(match.group(4)),
            }
        )
    bands.append(
        {
            "lower_bound_brl": _br_money(top.group(1)),
            "upper_bound_brl": None,
            "rate": _rate(top.group(2)),
            "deduction_brl": _br_money(top.group(3)),
        }
    )

    dependent = re.search(
        r"Dedução mensal por dependente:\s*R\$\s*([\d\.,]+)", monthly, re.IGNORECASE
    )
    simplified = re.search(
        r"Limite mensal de desconto simplificado:\s*R\$\s*([\d\.,]+)",
        monthly,
        re.IGNORECASE,
    )
    full_relief = re.search(
        r"até R\$\s*([\d\.,]+)\s*até R\$\s*([\d\.,]+)", reduction, re.IGNORECASE
    )
    phaseout = re.search(
        r"de R\$\s*([\d\.,]+)\s*até R\$\s*([\d\.,]+)\s*R\$\s*([\d\.,]+)\s*-\s*\(\s*([\d\.,]+)\s*x\s*rendimentos tributáveis sujeitos à incidência mensal\s*\)",
        reduction,
        re.IGNORECASE,
    )
    if not dependent or not simplified or not full_relief or not phaseout:
        raise ParserIncompatibleError("RFB IRRF scalar/reduction structure changed")

    return {
        "observation_type": f"rfb_irrf_{reference_year}",
        "reference_year": reference_year,
        "monthly_effective_from": f"{reference_year}-01-01",
        "monthly_table": bands,
        "dependent_deduction_brl": _br_money(dependent.group(1)),
        "simplified_discount_brl": _br_money(simplified.group(1)),
        "monthly_reduction": {
            "full_relief_income_upper_brl": _br_money(full_relief.group(1)),
            "max_reduction_brl": _br_money(full_relief.group(2)),
            "phaseout_income_lower_brl": _br_money(phaseout.group(1)),
            "phaseout_income_upper_brl": _br_money(phaseout.group(2)),
            "intercept_brl": _br_money(phaseout.group(3)),
            "slope": _decimal(phaseout.group(4)),
            "input_semantic": "rendimentos_tributaveis_sujeitos_incidencia_mensal",
        },
    }


# Backward-compatible import alias for historical callers and evidence tooling.
def parse_rfb_irrf_2026_snapshot(body: bytes) -> dict[str, JsonValue]:
    return parse_rfb_irrf_snapshot(body)
