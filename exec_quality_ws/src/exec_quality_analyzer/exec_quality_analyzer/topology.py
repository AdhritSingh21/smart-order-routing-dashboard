"""Execution-topology validation.

The OrderSubmitter and each per-venue ExecutionListener are separate ROS
nodes with separate parameters, so a user could configure a sandbox listener
while leaving the submitter in sim mode — the listener would then execute
real (sandbox) orders against SIMULATED reference prices and every slippage
number would be fiction.

validate_topology() is called by the OrderSubmitter BEFORE it creates its
submission timer (i.e. before any order can be published). It checks the
complete configured topology and raises TopologyConfigError on the first
incompatibility — startup fails loudly, nothing continues silently.

Rules
-----
- Every venue in `venues` needs a mode; modes must be sim|sandbox|production.
- production is refused anywhere unless allow_production_trading is true.
- A non-sim venue must receive venue-fetched reference quotes. If the
  submitter is in sim mode it can only produce simulated prices, so a
  non-sim venue under a sim submitter is a hard error — unless the
  explicitly named testing override `allow_sim_reference_for_live_testing`
  is set (and then every affected order is still labeled quote_mode='sim').
- A sim venue always uses simulated quotes (quote_mode='sim'), which is
  valid regardless of the submitter's own mode.
- Venues are validated independently, so mixed topologies (one sim venue,
  one sandbox venue) are fine as long as each venue is self-consistent.
- Comparability for ranking is resolved here per venue (see
  VenueConfig.ranking_comparable for the conservative defaults).

The all-sim default configuration validates with no credentials, no network
access, and no ccxt installed.
"""
from __future__ import annotations

from dataclasses import dataclass

from exec_quality_analyzer.adapters import VenueConfig


class TopologyConfigError(ValueError):
    """Raised when the configured execution topology is unsafe/inconsistent."""


VALID_MODES = ('sim', 'sandbox', 'production')
VALID_MARKET_DATA_MODES = ('venue', 'public')


@dataclass
class VenueTopology:
    venue: str
    execution_mode: str          # the venue listener's mode
    quote_mode: str              # 'sim' | 'venue' | 'public'
    market_data_mode: str        # 'venue' | 'public'
    ccxt_exchange_id: str
    comparable: bool             # include in cross-venue ranking?


def validate_topology(submitter_mode: str,
                      venue_specs: list[dict],
                      allow_production_trading: bool = False,
                      allow_sim_reference_for_live_testing: bool = False,
                      ) -> dict[str, VenueTopology]:
    """Validate the whole topology; returns per-venue resolved entries.

    venue_specs entries: {venue, mode, market_data_mode?, ccxt_exchange_id?,
    include_in_ranking?, synthetic_execution?}. Raises TopologyConfigError
    on any incompatibility.
    """
    if submitter_mode not in VALID_MODES:
        raise TopologyConfigError(
            f"submitter mode must be one of {VALID_MODES}, "
            f"got '{submitter_mode}'")
    if submitter_mode == 'production' and not allow_production_trading:
        raise TopologyConfigError(
            'submitter mode production requires explicit '
            'allow_production_trading: true')
    if not venue_specs:
        raise TopologyConfigError('at least one venue must be configured')

    out: dict[str, VenueTopology] = {}
    for spec in venue_specs:
        venue = (spec.get('venue') or '').strip()
        if not venue:
            raise TopologyConfigError('venue entry without a name')
        if venue in out:
            raise TopologyConfigError(f"duplicate venue '{venue}'")

        mode = spec.get('mode')
        if mode is None or str(mode).strip() == '':
            raise TopologyConfigError(
                f"venue '{venue}': missing mode configuration — every "
                f"configured venue needs an explicit (or inherited) mode")
        mode = str(mode).strip()
        if mode not in VALID_MODES:
            raise TopologyConfigError(
                f"venue '{venue}': invalid mode '{mode}' "
                f"(must be one of {VALID_MODES})")
        if mode == 'production' and not allow_production_trading:
            raise TopologyConfigError(
                f"venue '{venue}': production mode is blocked without "
                f"explicit allow_production_trading: true")

        md_mode = str(spec.get('market_data_mode') or 'venue').strip()
        if md_mode not in VALID_MARKET_DATA_MODES:
            raise TopologyConfigError(
                f"venue '{venue}': invalid market_data_mode '{md_mode}' "
                f"(must be one of {VALID_MARKET_DATA_MODES})")

        ex_id = str(spec.get('ccxt_exchange_id') or '').strip()
        if mode != 'sim' and not ex_id:
            raise TopologyConfigError(
                f"venue '{venue}': ccxt_exchange_id is required for "
                f"mode '{mode}'")

        if mode == 'sim':
            quote_mode = 'sim'
        else:
            # A non-sim listener must get venue-fetched reference quotes.
            if submitter_mode == 'sim':
                if not allow_sim_reference_for_live_testing:
                    raise TopologyConfigError(
                        f"venue '{venue}' runs in {mode} mode but the "
                        f"submitter is in sim mode: {mode} executions would "
                        f"be measured against SIMULATED reference prices. "
                        f"Set the submitter mode to '{mode}', or set "
                        f"allow_sim_reference_for_live_testing: true if "
                        f"this is intentional test wiring.")
                quote_mode = 'sim'
            else:
                quote_mode = md_mode

        # Comparability via the same conservative policy as VenueConfig.
        cfg = VenueConfig(
            venue=venue, mode=mode,
            ccxt_exchange_id=ex_id or 'unused',
            market_data_mode=md_mode if mode != 'sim' else 'venue',
            allow_production_trading=allow_production_trading,
            synthetic_execution=bool(spec.get('synthetic_execution', False)),
            include_in_ranking=spec.get('include_in_ranking'),
        )
        comparable = cfg.ranking_comparable and quote_mode != 'sim' \
            if mode != 'sim' else cfg.ranking_comparable

        out[venue] = VenueTopology(
            venue=venue, execution_mode=mode, quote_mode=quote_mode,
            market_data_mode=md_mode, ccxt_exchange_id=ex_id,
            comparable=comparable)
    return out
