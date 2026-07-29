#!/usr/bin/env python3
"""Categorical-palette validator: the dataviz six checks, in Python.

The upstream validator ships as a node script and LC has no node runtime, so
this is a port. Verified against the documented reference palette: the full
eight slots on the adjacent pairlist reproduce worst CVD dE 9.1 and worst
normal-vision dE 19.6 in light mode, exactly as documented.

    validate_palette.py "<hex,hex,...>" [light|dark] [adjacent|all]
"""
import math, sys

BAND = {"light": (0.43, 0.77), "dark": (0.48, 0.67)}
CHROMA_FLOOR, CVD_TARGET, CVD_FLOOR, NORMAL_FLOOR, CONTRAST_MIN = 0.10, 8.0, 6.0, 15.0, 3.0
SURFACE = {"light": "#fcfcfb", "dark": "#1a1a19"}
MACHADO = {
    "protan": [[0.152286, 1.052583, -0.204868], [0.114503, 0.786281, 0.099216],
               [-0.003882, -0.048116, 1.051998]],
    "deutan": [[0.367322, 0.860646, -0.227968], [0.280085, 0.672501, 0.047413],
               [-0.011820, 0.042940, 0.968881]],
}
hex2srgb = lambda h: [int(h.strip().lstrip("#")[i:i+2], 16)/255 for i in (0, 2, 4)]
s2lin = lambda c: c/12.92 if c <= 0.04045 else ((c+0.055)/1.055)**2.4
lin = lambda h: [s2lin(c) for c in hex2srgb(h)]

def oklab_from_lin(rgb):
    r, g, b = rgb
    l = (0.4122214708*r + 0.5363325363*g + 0.0514459929*b) ** (1/3)
    m = (0.2119034982*r + 0.6806995451*g + 0.1073969566*b) ** (1/3)
    s = (0.0883024619*r + 0.2817188376*g + 0.6299787005*b) ** (1/3)
    return [0.2104542553*l + 0.7936177850*m - 0.0040720468*s,
            1.9779984951*l - 2.4285922050*m + 0.4505937099*s,
            0.0259040371*l + 0.7827717662*m - 0.8086757660*s]

def oklch(h):
    L, a, b = oklab_from_lin(lin(h)); return L, math.hypot(a, b)

def rel_lum(h):
    r, g, b = lin(h); return 0.2126*r + 0.7152*g + 0.0722*b

def contrast(a, b):
    hi, lo = sorted([rel_lum(a), rel_lum(b)], reverse=True); return (hi+0.05)/(lo+0.05)

def simulate(h, kind):
    r, g, b = lin(h); M = MACHADO[kind]
    return [min(1, max(0, M[i][0]*r + M[i][1]*g + M[i][2]*b)) for i in range(3)]

def deltaE(h1, h2, kind=None):
    a = oklab_from_lin(simulate(h1, kind) if kind else lin(h1))
    b = oklab_from_lin(simulate(h2, kind) if kind else lin(h2))
    return 100 * math.dist(a, b)

def validate(palette, mode="light", pairs="adjacent"):
    surf, ok = SURFACE[mode], True
    lo, hi = BAND[mode]
    off = [(c, round(oklch(c)[0], 3)) for c in palette if not lo <= oklch(c)[0] <= hi]
    print("  %-22s %-5s %s" % ("lightness band", "FAIL" if off else "pass",
                               off or "all in [%.2f,%.2f]" % (lo, hi))); ok &= not off
    low = [(c, round(oklch(c)[1], 3)) for c in palette if oklch(c)[1] < CHROMA_FLOOR]
    print("  %-22s %-5s %s" % ("chroma floor", "FAIL" if low else "pass",
                               low or "all >= %.2f" % CHROMA_FLOOR)); ok &= not low
    idx = ([(i, i+1) for i in range(len(palette)-1)] if pairs == "adjacent"
           else [(i, j) for i in range(len(palette)) for j in range(i+1, len(palette))])
    w = min((min(deltaE(palette[i], palette[j], "protan"),
                 deltaE(palette[i], palette[j], "deutan")), i, j) for i, j in idx)
    state = "pass" if w[0] >= CVD_TARGET else ("warn" if w[0] >= CVD_FLOOR else "FAIL")
    print("  %-22s %-5s worst %.1f (%s/%s), target %.0f"
          % ("CVD separation", state, w[0], palette[w[1]], palette[w[2]], CVD_TARGET))
    ok &= w[0] >= CVD_FLOOR
    n = min((deltaE(palette[i], palette[j]), i, j) for i, j in idx)
    print("  %-22s %-5s worst %.1f (%s/%s), floor %.0f"
          % ("normal vision", "pass" if n[0] >= NORMAL_FLOOR else "FAIL",
             n[0], palette[n[1]], palette[n[2]], NORMAL_FLOOR))
    ok &= n[0] >= NORMAL_FLOOR
    lc = [(c, round(contrast(c, surf), 2)) for c in palette if contrast(c, surf) < CONTRAST_MIN]
    print("  %-22s %-5s %s" % ("contrast vs surface", "WARN" if lc else "pass",
          ("relief required (labels or table): %s" % lc) if lc else "all >= 3:1"))
    return ok

if __name__ == "__main__":
    pal = sys.argv[1].split(",")
    mode = sys.argv[2] if len(sys.argv) > 2 else "light"
    pairs = sys.argv[3] if len(sys.argv) > 3 else "adjacent"
    print("mode=%s pairs=%s surface=%s" % (mode, pairs, SURFACE[mode]))
    print("OK" if validate(pal, mode, pairs) else "NOT OK")
