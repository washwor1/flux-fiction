# Flux Fiction Agent Instructions

## Project purpose

Flux Fiction is a trace-driven emulator that runs emulated jobs through
real Flux scheduling components.

## Read before editing

For changes involving:

- configuration: read `docs/reference/configuration.md`
- simulation behavior: read `docs/concepts/simulation-lifecycle.md`
- Flux synchronization: read `docs/concepts/time-and-quiescence.md`
- jobtap: read `docs/reference/components/jobtap.md`
- resources: read `docs/concepts/resource-model.md`

## Repository map

- `src/flux_fiction/api/`: public configuration and execution API
- `src/flux_fiction/_core/`: simulation engine, events, and job models
- `src/flux_fiction/_adapters/`: backend integrations
- `src/flux_fiction/_exec/`: emulated execution services
- `src/flux_fiction/_outputs/`: output artifact generation
- `src/jobtap/`: native Flux jobtap plugin
- `src/tests/`: automated tests

## Required validation

Run:

    pytest -q

For behavior affecting Flux integration, also run:

    flux-fiction-run test/simple_test/config.toml --tag smoke --no-faketime

## Documentation rules

- Do not infer undocumented semantics from variable names.
- Cite the relevant source file and symbol when adding technical claims.
- Flag contradictions between CLI help, configuration validation, and runtime use.
- Update configuration reference when configuration fields change.
- Update output reference when emitted artifacts change.
- Commands in user guides must be executed before being documented.
- Mark experimental or incomplete behavior explicitly.

## Coding constraints

- Preserve deterministic simulation ordering.
- Do not advance simulation time until the quiescence contract is satisfied.
- Do not introduce direct Flux dependencies into the core engine when an
  adapter method can provide the behavior.