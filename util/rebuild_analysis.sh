#!/usr/bin/env bash
# Rebuild the analysis notebooks against whatever the campaign has produced.
#
#   util/rebuild_analysis.sh            # incremental harvest, then re-execute both
#   util/rebuild_analysis.sh --force    # re-parse every run, ignoring the cache
#
# Three steps: re-harvest all clusters, regenerate the notebooks from source,
# re-execute them in place so the committed .ipynb carries current outputs.
# Safe to run while the campaign is still going.
set -uo pipefail

REPO="${FF_REPO:-/usr/WS1/ashworth12/ff-podman/flux-fiction-develop}"
OUT="${FF_ANALYSIS_DIR:-/usr/WS1/ashworth12/ff-podman/rabbit-threshold-analysis}"
PY="${FF_PYTHON:-/collab/usr/gapps/python/toss_4_x86_64_ib/anaconda3-2025.3.1/bin/python3}"
NOTEBOOKS=(01-performance.ipynb 02-schedule-data.ipynb)

echo "== 1/3  harvesting all clusters"
"$PY" "$REPO/util/ff_harvest.py" "$@" || exit 1

echo
echo "== 2/3  regenerating notebooks"
"$PY" "$REPO/util/build_notebooks.py" || exit 1

echo
echo "== 3/3  executing notebooks"
cd "$OUT" || exit 1
for nb in "${NOTEBOOKS[@]}"; do
    "$PY" -m nbconvert --to notebook --execute --inplace \
        --ExecutePreprocessor.timeout=1800 "$nb" 2>&1 | sed 's/^/   /'
done

# Fail loudly rather than leaving a notebook full of tracebacks looking finished.
"$PY" - "${NOTEBOOKS[@]}" <<'EOF'
import sys, nbformat
bad = 0
for path in sys.argv[1:]:
    nb = nbformat.read(path, as_version=4)
    figs = errs = 0
    for cell in nb.cells:
        for out in cell.get("outputs", []) if cell.cell_type == "code" else []:
            if out.output_type == "error":
                errs += 1
                print(f"   {path}: {out.ename}: {out.evalue}")
            figs += "image/png" in out.get("data", {})
    print(f"   {path}: {figs} figures, {errs} errors")
    bad += errs
sys.exit(1 if bad else 0)
EOF
