#!/usr/bin/env python3
"""Build an executable plotnine notebook for the accuracy controls."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from textwrap import dedent

import nbformat as nbf


def md(source: str):
    return nbf.v4.new_markdown_cell(dedent(source).strip() + "\n")


def code(source: str):
    return nbf.v4.new_code_cell(dedent(source).strip() + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    root = args.root.absolute()
    output = args.output.absolute()
    report = json.loads(
        (root / "reports" / "accuracy_controls.json").read_text(encoding="utf-8")
    )
    if not report["complete"] or not report["all_traces_valid"]:
        raise SystemExit("accuracy controls or trace validation are incomplete")

    rows = report["runs"]
    by_key = {
        (row["policy"], row["scheduler_base"], row["account_system_latency"]): row
        for row in rows
    }
    findings = []
    for policy in ("easy", "conservative", "hybrid"):
        on_current = by_key[(policy, "v053", True)][
            "notebook_utilization_error_pct_points"
        ]
        off_current = by_key[(policy, "v053", False)][
            "notebook_utilization_error_pct_points"
        ]
        on_old = by_key[(policy, "v048", True)][
            "notebook_utilization_error_pct_points"
        ]
        off_old = by_key[(policy, "v048", False)][
            "notebook_utilization_error_pct_points"
        ]
        findings.append(
            f"- **{policy.title()}** — current: latency on `{on_current:.3f}` pp, "
            f"off `{off_current:.3f}` pp; v0.48: on `{on_old:.3f}` pp, "
            f"off `{off_old:.3f}` pp."
        )
    best = min(rows, key=lambda row: row["notebook_utilization_error_pct_points"])
    findings.extend(
        [
            "- Every arm completed 500/500 jobs and every merged trace retained "
            "`run_match` job-ID metadata.",
            f"- The lowest observed error was **{best['notebook_utilization_error_pct_points']:.3f} pp** "
            f"for `{best['name']}`.",
            "- These are paired controls under the modified shared clock and level-2 "
            "DFTracer. They isolate the requested factors; they do not test synchronous submission.",
        ]
    )
    findings_text = "\n".join(findings)

    cells = [
        md(
            """
            # Flux Fiction accuracy controls: system latency and Fluxion base

            This report isolates the two requested accuracy factors using the
            policy-specific 500-job Salishan Gaussian traces:

            - `account_system_latency = true` versus `false`
            - current v0.53-derived Fluxion versus reconstructed v0.48

            Submission stays asynchronous. All jobspecs include cores. Easy and
            Conservative reservation depth are not experimental factors; the
            frozen Salishan scheduler configurations are used unchanged. Hybrid
            uses reservation depth 4. Fluxion is Release/O3 with DFTracer level 2.
            Each pdebug allocation runs one current/v0.48 pair concurrently.
            """
        ),
        code(
            f"""
            from pathlib import Path
            import json
            import pandas as pd
            from IPython.display import display
            from plotnine import (
                aes, element_text, facet_wrap, geom_col, geom_hline, ggplot,
                labs, position_dodge, scale_fill_manual, theme, theme_bw,
            )

            ROOT = Path({str(root)!r})
            payload = json.loads((ROOT / 'reports' / 'accuracy_controls.json').read_text())
            assert payload['complete']
            assert payload['all_traces_valid']
            runs = pd.DataFrame(payload['runs'])
            runs['latency_setting'] = runs['account_system_latency'].map({{
                True: 'account latency on', False: 'account latency off'
            }})
            runs['policy'] = pd.Categorical(
                runs['policy'], ['easy', 'conservative', 'hybrid'], ordered=True
            )
            assert len(runs) == 12
            assert (runs['state'] == 'succeeded').all()
            assert (runs['simulated_jobs'] == 500).all()
            assert (runs['run_match_events_with_jobid'] > 0).all()
            display(runs[[
                'policy', 'scheduler_base', 'account_system_latency',
                'notebook_utilization_error_pct_points',
                'utilization_curve_mae_pct_points', 'wall_seconds',
                'run_match_events_with_jobid', 'accuracy_pass_1pp',
            ]].sort_values(['policy', 'account_system_latency', 'scheduler_base']).round(3))
            """
        ),
        md(
            """
            ## Accuracy result

            The dashed line is the Gaussian acceptance threshold used by the
            Salishan suite: 1 percentage point of notebook-equivalent mean
            utilization error. Shorter bars are more accurate.
            """
        ),
        code(
            """
            (
                ggplot(
                    runs,
                    aes(
                        x='latency_setting',
                        y='notebook_utilization_error_pct_points',
                        fill='scheduler_base',
                    ),
                )
                + geom_col(position=position_dodge(width=0.8), width=0.72)
                + geom_hline(yintercept=1.0, linetype='dashed', color='#9B2226')
                + facet_wrap('~policy', nrow=1)
                + scale_fill_manual(values={'v053': '#4472C4', 'v048': '#ED7D31'})
                + labs(
                    title='System-latency accounting and scheduler base affect accuracy differently',
                    x='Flux Fiction setting',
                    y='Notebook-equivalent utilization error (percentage points)',
                    fill='Fluxion base',
                )
                + theme_bw()
                + theme(
                    figure_size=(12, 4.5),
                    axis_text_x=element_text(rotation=15, ha='right'),
                )
            )
            """
        ),
        md(
            """
            ## Isolated factor effects

            Negative change means the factor reduced accuracy error. This table
            keeps the other factor fixed for every comparison.
            """
        ),
        code(
            """
            effects = pd.DataFrame(payload['factor_effects'])
            display(effects.round(4))

            effect_plot = effects.copy()
            effect_plot['comparison'] = effect_plot['factor'].map({
                'turn_account_system_latency_off': 'turn latency accounting off',
                'switch_v053_to_v048': 'switch current to v0.48',
            })
            effect_plot['condition'] = effect_plot.apply(
                lambda row: row.get('scheduler_base')
                if pd.notna(row.get('scheduler_base'))
                else ('latency on' if row.get('account_system_latency') else 'latency off'),
                axis=1,
            )
            (
                ggplot(effect_plot, aes('condition', 'error_change_pp', fill='comparison'))
                + geom_col(position=position_dodge(width=0.8), width=0.72)
                + geom_hline(yintercept=0.0, color='#333333')
                + facet_wrap('~policy', nrow=1)
                + scale_fill_manual(values={
                    'turn latency accounting off': '#2A9D8F',
                    'switch current to v0.48': '#E76F51',
                })
                + labs(
                    title='Change in utilization error from each isolated factor',
                    x='Factor held fixed / conditioning arm',
                    y='Error change (percentage points; negative is better)',
                    fill='Changed factor',
                )
                + theme_bw()
                + theme(
                    figure_size=(12, 4.8),
                    axis_text_x=element_text(rotation=20, ha='right'),
                )
            )
            """
        ),
        md(
            """
            ## Runtime and trace integrity

            Wall time is diagnostic rather than an accuracy metric. Every arm
            used O3 and DFTracer level 2, and each trace was accepted only after
            finding `run_match` events carrying job IDs.
            """
        ),
        code(
            """
            runs['wall_minutes'] = runs['wall_seconds'] / 60.0
            (
                ggplot(runs, aes('latency_setting', 'wall_minutes', fill='scheduler_base'))
                + geom_col(position=position_dodge(width=0.8), width=0.72)
                + facet_wrap('~policy', nrow=1, scales='free_y')
                + scale_fill_manual(values={'v053': '#4472C4', 'v048': '#ED7D31'})
                + labs(
                    title='Real child wall time for each 500-job control',
                    x='Flux Fiction setting', y='Wall time (minutes)', fill='Fluxion base',
                )
                + theme_bw()
                + theme(
                    figure_size=(12, 4.5),
                    axis_text_x=element_text(rotation=15, ha='right'),
                )
            )
            """
        ),
        md(f"## Findings\n\n{findings_text}"),
    ]

    notebook = nbf.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3"},
        },
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.execute:
        from nbclient import NotebookClient

        NotebookClient(notebook, timeout=600, kernel_name="python3").execute()
    nbf.write(notebook, output)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
