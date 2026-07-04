from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class VenueReportPayload(BaseModel):
    venue: str = Field(min_length=1)
    window_orders: int = Field(ge=0)
    fill_rate: float = Field(ge=0.0, le=1.0)
    slippage_bps_p50: float
    slippage_bps_p95: float
    latency_ms_p50: float = Field(ge=0.0)
    latency_ms_p95: float = Field(ge=0.0)
    latency_ms_p99: float = Field(ge=0.0)
    comparable: bool = True
    timestamp: float = Field(ge=0.0)


class MetricPayload(BaseModel):
    order_id: str = Field(min_length=1)
    venue: str = Field(min_length=1)
    symbol: str
    status: str
    slippage_bps: float
    latency_ms: float = Field(ge=0.0)
    fill_ratio: float = Field(ge=0.0, le=1.0)
    filled: bool
    requested_quantity: float = Field(ge=0.0)
    filled_quantity: float = Field(ge=0.0)
    avg_fill_price: float = Field(ge=0.0)
    reference_price: float = Field(ge=0.0)
    exchange_status: str
    terminal_reason: str
    quote_mode: str
    execution_mode: str
    comparable: bool = True
    timestamp: float = Field(ge=0.0)


class VenueReportEnvelope(BaseModel):
    type: Literal["venue_report"]
    data: VenueReportPayload


class MetricEnvelope(BaseModel):
    type: Literal["metric"]
    data: MetricPayload


IngestEnvelope = Annotated[
    Union[VenueReportEnvelope, MetricEnvelope],
    Field(discriminator="type"),
]
