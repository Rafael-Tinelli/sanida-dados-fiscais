from __future__ import annotations

RFB_SOURCE_ID = "RFB_IRRF_TABLE_CURRENT"
INSS_SOURCE_ID = "INSS_TABLE_CURRENT"

LEGACY_SOURCE_ID_ALIASES: dict[str, str] = {
    "RFB_IRRF_TABLE_2026": RFB_SOURCE_ID,
    "INSS_TABLE_2026": INSS_SOURCE_ID,
}

CANONICAL_TO_LEGACY_SOURCE_IDS: dict[str, tuple[str, ...]] = {
    RFB_SOURCE_ID: ("RFB_IRRF_TABLE_2026",),
    INSS_SOURCE_ID: ("INSS_TABLE_2026",),
}


def canonical_source_id(source_id: str) -> str:
    return LEGACY_SOURCE_ID_ALIASES.get(source_id, source_id)


def legacy_source_ids_for(source_id: str) -> tuple[str, ...]:
    return CANONICAL_TO_LEGACY_SOURCE_IDS.get(canonical_source_id(source_id), ())


def source_ids_equivalent(left: str, right: str) -> bool:
    return canonical_source_id(left) == canonical_source_id(right)
