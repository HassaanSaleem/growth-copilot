"""growth-copilot: a LangGraph orchestrator for product analytics.

Natural-language questions become auditable, dependency-aware task graphs
("recipes") that are grounded against warehouse metadata and executed in
parallel against an embedded DuckDB warehouse. Only aggregates ever leave
the warehouse; the LLM plans and narrates, code computes.
"""

__version__ = "0.1.0"
