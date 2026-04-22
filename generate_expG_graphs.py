#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_expG_graphs.py
=======================
Generates three slide-style figures for Experiment G:
  figG_kill_leader.png  — election latency + phase breakdown (slide 1 style)
  figG_topology.png     — partition topology diagram (slide 2 style)
  figG_timing.png       — partition failure/heal timing charts (slide 3 style)

Run from project root:  python generate_expG_graphs.py
Output: graphs/figG_*.png
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np

os.makedirs("graphs", exist_ok=True)

plt.rcParams.update({
    "font.family"       : "DejaVu Sans",
    "font.size"         : 10,
    "axes.titlesize"    : 12,
    "axes.titleweight"  : "bold",
    "axes.labelsize"    : 10,
    "axes.spines.top"   : False,
    "axes.spines.right" : False,
    "figure.facecolor"  : "white",
    "axes.facecolor"    : "white",
    "axes.grid"         : True,
    "grid.color"        : "#E8E8E8",
    "grid.linewidth"    : 0.8,
    "figure.dpi"        : 150,
    "savefig.bbox"      : "tight",
    "savefig.facecolor" : "white",
})

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

# Kill Leader (Experiment A, v2) — from results_v2_live.csv + logs
kill_election   = [3.791, 3.359, 3.650]           # B-A per run
kill_leaders    = ["zk-2 killed", "zk-1 killed", "zk-2 killed"]
kill_total_v2   = [64.2, 64.1, 67.7]              # total recovery seconds
kill_total_v1   = [64.1, 67.8, 62.5]              # v1 runs 1-3 (5s polling)

# Phase breakdown for v2 kill leader stacked bars
# Phase 1: election latency (from logs)
# Phase 2: pod restart + ZK sync (from log observation: ~12s runs 1-2, ~9.6s run 3)
# Phase 3: stable under fault (remainder of 60s window)
# Phase 4: post-removal polling until confirmed
P1 = kill_election                                 # [3.791, 3.359, 3.650]
P2 = [12.6, 12.6, 9.6]                            # pod restart + rejoin
P3 = [60 - p1 - p2 for p1, p2 in zip(P1, P2)]    # [43.6, 44.0, 46.75]
P4 = [t - 60 for t in kill_total_v2]              # [4.2, 4.1, 7.7]

# Experiment G data — leader on minority partition (3 runs)
# B-A = new majority leader elected (seconds from partition inject)
B_A = [4.870, 5.103, 4.848]
# D-A = minority quorum lost (seconds from partition inject)
D_A = [6.358, 5.103, 6.289]
# F-E = first write after partition healed (seconds from removal)
F_E = [33.3, 0.5, 33.2]

B_mean  = np.mean(B_A)   # 4.940
D_mean  = np.mean(D_A)   # 5.917
FE_mean = np.mean(F_E)   # 22.33

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
MINORITY_BG     = "#FDECEA"
MINORITY_BORDER = "#E57373"
MINORITY_DARK   = "#C0392B"   # former leader box
MINORITY_MID    = "#E88080"   # follower box (minority)
MAJORITY_BG     = "#E8F5E9"
MAJORITY_BORDER = "#66BB6A"
MAJORITY_DARK   = "#2D7D52"   # new leader box
MAJORITY_MID    = "#52A96A"   # follower boxes (majority)
PARTITION_CLR   = "#E53935"

PH1_CLR  = "#C0392B"   # Phase 1 — election (dark red)
PH2_CLR  = "#E67E22"   # Phase 2 — pod restart (orange)
PH3_CLR  = "#F5A58A"   # Phase 3 — stable under fault (light salmon)
PH4_CLR  = "#FAD7CC"   # Phase 4 — post-removal confirm (very light)
V1_CLR   = "#AAB7B8"   # v1 total recovery bar (grey)

BAR_D = "#C0392B"       # First Failed Write — Minority (brick red)
BAR_B = "#B7770D"       # New Leader Elected — Majority (amber)
BAR_C = "#27AE60"       # First Write — Majority (green)
BAR_F = "#2980B9"       # First Write After Heal (blue)

MEAN_CLR = "#5D6D7E"    # dashed mean line colour


def save(fig, name):
    path = os.path.join("graphs", name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


# ===========================================================================
# FIGURE 1 — Kill Leader: Election Latency + Phase Breakdown (slide 1 style)
# ===========================================================================
def fig_kill_leader():
    fig, (ax_left, ax_right) = plt.subplots(
        1, 2, figsize=(13, 5.5),
        gridspec_kw={"width_ratios": [1, 1.4]},
    )
    fig.suptitle("Test 1: Kill Leader", fontsize=15, fontweight="bold",
                 x=0.04, ha="left")

    # ── Left panel: election latency per run ─────────────────────────────
    xs    = np.arange(3)
    mean  = np.mean(kill_election)
    sd    = np.std(kill_election)
    bar_c = [PH1_CLR, PH2_CLR, "#E8943C"]   # slight variation per run

    bars = ax_left.bar(xs, kill_election, color=bar_c, width=0.52,
                       zorder=3, edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars, kill_election):
        ax_left.text(bar.get_x() + bar.get_width() / 2,
                     val + 0.05, f"{val:.3f}s",
                     ha="center", va="bottom", fontsize=10.5,
                     fontweight="bold", color="#2C3E50")

    # Mean ± SD band
    ax_left.axhline(mean, color=MEAN_CLR, linewidth=1.6,
                    linestyle="--", zorder=4, label=f"Mean = {mean:.3f}s")
    ax_left.axhspan(mean - sd, mean + sd, alpha=0.12, color=MEAN_CLR,
                    zorder=2, label=f"±1 SD (SD = {sd:.3f}s)")

    ax_left.set_xticks(xs)
    ax_left.set_xticklabels(
        [f"Run {i+1}\n({lbl})" for i, lbl in enumerate(kill_leaders)],
        fontsize=9.5,
    )
    ax_left.set_ylim(0, 7.2)
    ax_left.set_ylabel("Election latency (seconds)")
    ax_left.set_title("Leader Election Latency — per run\n"
                      "Time from fault injection to new leader confirmed\n"
                      "(1-second polling resolution)")
    ax_left.legend(fontsize=9, loc="upper left")
    ax_left.yaxis.set_minor_locator(plt.MultipleLocator(0.5))

    # Election Latency Summary text box — upper right, above bars
    summary = (
        f"Election Latency Summary\n"
        f"{'─'*28}\n"
        f"Min:     {min(kill_election):.3f} s\n"
        f"Mean:    {mean:.3f} s\n"
        f"Max:     {max(kill_election):.3f} s\n"
        f"Std Dev: {sd:.3f} s\n"
        f"Runs:    3 / 3 (100% success)\n"
        f"Quorum lost: Never (2/3 survive)"
    )
    ax_left.text(
        0.97, 0.97, summary,
        transform=ax_left.transAxes,
        fontsize=9, va="top", ha="right",
        fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.6", facecolor="#F0F3F4",
                  edgecolor="#AAB7B8", linewidth=1.2),
    )

    # ── Right panel: stacked phase breakdown (v2) + v1 comparison ────────
    run_xs  = np.arange(3)
    width   = 0.32
    offsets = [-0.20, 0.20]

    # v1 bars (single colour — total recovery, no phase detail)
    v1_bars = ax_right.bar(run_xs + offsets[0], kill_total_v1,
                            width, color=V1_CLR, zorder=3,
                            edgecolor="white", linewidth=0.5,
                            label="v1 — total recovery (5s polling, no phase detail)")
    for bar, val in zip(v1_bars, kill_total_v1):
        ax_right.text(bar.get_x() + bar.get_width() / 2,
                      val / 2, f"{val:.1f}s",
                      ha="center", va="center", fontsize=8.5,
                      color="white", fontweight="bold")

    # v2 stacked bars
    bottoms = [0, 0, 0]
    phase_data   = [P1, P2, P3, P4]
    phase_colors = [PH1_CLR, PH2_CLR, PH3_CLR, PH4_CLR]
    phase_labels = ["Phase 1: Election (~4s)",
                    "Phase 2: Pod restart & rejoin",
                    "Phase 3: Stable under fault",
                    "Phase 4: Post removal confirm"]

    for ph_vals, ph_clr, ph_lbl in zip(phase_data, phase_colors, phase_labels):
        segs = ax_right.bar(run_xs + offsets[1], ph_vals,
                             width, bottom=bottoms, color=ph_clr,
                             zorder=3, edgecolor="white", linewidth=0.5,
                             label=ph_lbl)
        # Label Phase 1 and Phase 2 segments
        if ph_lbl.startswith("Phase 1"):
            for bar, bval, bot in zip(segs, ph_vals, bottoms):
                ax_right.text(bar.get_x() + bar.get_width() / 2,
                              bot + bval / 2, f"{bval:.1f}s",
                              ha="center", va="center", fontsize=7.5,
                              color="white", fontweight="bold")
        if ph_lbl.startswith("Phase 2"):
            for bar, bval, bot in zip(segs, ph_vals, bottoms):
                ax_right.text(bar.get_x() + bar.get_width() / 2,
                              bot + bval / 2, f"{bval:.1f}s",
                              ha="center", va="center", fontsize=7.5,
                              color="white", fontweight="bold")
        bottoms = [b + v for b, v in zip(bottoms, ph_vals)]

    # Total labels on top of v2 bars
    for i, (total, bot) in enumerate(zip(kill_total_v2, bottoms)):
        ax_right.text(run_xs[i] + offsets[1], total + 0.8,
                      f"{total:.1f}s", ha="center", va="bottom",
                      fontsize=9.5, fontweight="bold", color="#2C3E50")

    ax_right.set_xticks(run_xs)
    ax_right.set_xticklabels(
        [f"Run {i+1}\n({lbl})" for i, lbl in enumerate(kill_leaders)],
        fontsize=9,
    )
    ax_right.set_ylim(0, 82)
    ax_right.set_ylabel("Recovery time (seconds from fault injection)")
    ax_right.set_title("Recovery Phase Breakdown (v2) vs v1 Total\n"
                       "Left bar = v1 (grey)  ·  Right bar = v2 phases (coloured)")
    ax_right.legend(fontsize=8.5, loc="upper right", framealpha=0.95)

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save(fig, "figG_kill_leader.png")


# ===========================================================================
# FIGURE 2 — Topology Diagram (slide 2 style)
# ===========================================================================
def fig_topology():
    """
    Representative run: Run 1 / Run 3 (most common outcome)
      Minority: zk-4 (former leader, isolated) + zk-0 (follower, no quorum)
      Majority: zk-1 (new leader, elected) + zk-2 + zk-3 (followers)
    """
    fig, ax = plt.subplots(figsize=(13, 7.5))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 8)
    ax.axis("off")
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")

    # Title
    ax.text(6.5, 7.55,
            "System Recovery with Leader on Minority Side",
            ha="center", va="center",
            fontsize=16, fontweight="bold",
            bbox=dict(boxstyle="square,pad=0.5", facecolor="white",
                      edgecolor="black", linewidth=1.8))

    # ── Minority panel ────────────────────────────────────────────────────
    minor_bg = FancyBboxPatch((0.25, 0.35), 4.9, 6.3,
                               boxstyle="round,pad=0.15",
                               facecolor=MINORITY_BG,
                               edgecolor=MINORITY_BORDER,
                               linewidth=2, zorder=1)
    ax.add_patch(minor_bg)
    ax.text(2.7, 6.35, "Minority partition",
            ha="center", va="center",
            fontsize=13, color="#B71C1C")

    # zk-4 former leader
    zk4 = FancyBboxPatch((0.6, 4.35), 4.2, 1.65,
                          boxstyle="round,pad=0.18",
                          facecolor=MINORITY_DARK, edgecolor=MINORITY_DARK,
                          linewidth=0, zorder=2)
    ax.add_patch(zk4)
    ax.text(2.7, 5.27, "zk-4", ha="center", va="center",
            fontsize=22, fontweight="bold", color="white")
    ax.text(2.7, 4.68, "former leader · isolated",
            ha="center", va="center", fontsize=11, color="white")

    # Dashed connector zk-4 → zk-0
    ax.plot([2.7, 2.7], [4.32, 3.5], color="#E57373",
            linewidth=1.8, linestyle="--", zorder=3)

    # zk-0 follower (minority)
    zk0 = FancyBboxPatch((0.6, 2.25), 4.2, 1.15,
                          boxstyle="round,pad=0.18",
                          facecolor=MINORITY_MID, edgecolor=MINORITY_MID,
                          linewidth=0, zorder=2)
    ax.add_patch(zk0)
    ax.text(2.7, 2.97, "zk-0", ha="center", va="center",
            fontsize=19, fontweight="bold", color="white")
    ax.text(2.7, 2.48, "follower · no quorum",
            ha="center", va="center", fontsize=11, color="white")

    # Writes rejected
    wr = FancyBboxPatch((0.6, 0.65), 4.2, 1.35,
                         boxstyle="round,pad=0.18",
                         facecolor="white", edgecolor=MINORITY_BORDER,
                         linewidth=1.8, zorder=2)
    ax.add_patch(wr)
    ax.text(2.7, 1.42, "writes rejected",
            ha="center", va="center",
            fontsize=12, fontweight="bold", color="#B71C1C")
    ax.text(2.7, 0.95, "2 of 5 — no quorum",
            ha="center", va="center", fontsize=11, color="#666666")

    # ── Partition line ────────────────────────────────────────────────────
    ax.plot([6.25, 6.25], [0.2, 7.2],
            color=PARTITION_CLR, linewidth=2.8,
            linestyle="--", zorder=4, alpha=0.85)
    ax.text(6.25, 3.75, "partition",
            ha="center", va="center", fontsize=11,
            color=PARTITION_CLR, fontweight="bold", rotation=0,
            bbox=dict(boxstyle="round,pad=0.35",
                      facecolor="white",
                      edgecolor=PARTITION_CLR, linewidth=1.8))

    # ── Majority panel ────────────────────────────────────────────────────
    major_bg = FancyBboxPatch((7.1, 0.35), 5.6, 6.3,
                               boxstyle="round,pad=0.15",
                               facecolor=MAJORITY_BG,
                               edgecolor=MAJORITY_BORDER,
                               linewidth=2, zorder=1)
    ax.add_patch(major_bg)
    ax.text(9.9, 6.35, "Majority partition · quorum",
            ha="center", va="center",
            fontsize=13, color="#1B5E20")

    # zk-1 new leader
    zk1 = FancyBboxPatch((7.5, 4.35), 4.8, 1.65,
                          boxstyle="round,pad=0.18",
                          facecolor=MAJORITY_DARK, edgecolor=MAJORITY_DARK,
                          linewidth=0, zorder=2)
    ax.add_patch(zk1)
    ax.text(9.9, 5.27, "zk-1", ha="center", va="center",
            fontsize=22, fontweight="bold", color="white")
    ax.text(9.9, 4.68, "new leader (elected)",
            ha="center", va="center", fontsize=11, color="white")

    # Lines zk-1 → followers
    ax.plot([8.8, 8.8], [4.32, 3.5], color="#66BB6A",
            linewidth=1.8, zorder=3)
    ax.plot([11.0, 11.0], [4.32, 3.5], color="#66BB6A",
            linewidth=1.8, zorder=3)

    # zk-2 follower
    zk2 = FancyBboxPatch((7.5, 2.25), 2.2, 1.15,
                          boxstyle="round,pad=0.18",
                          facecolor=MAJORITY_MID, edgecolor=MAJORITY_MID,
                          linewidth=0, zorder=2)
    ax.add_patch(zk2)
    ax.text(8.6, 2.97, "zk-2", ha="center", va="center",
            fontsize=17, fontweight="bold", color="white")
    ax.text(8.6, 2.48, "follower",
            ha="center", va="center", fontsize=11, color="white")

    # zk-3 follower
    zk3 = FancyBboxPatch((9.85, 2.25), 2.2, 1.15,
                          boxstyle="round,pad=0.18",
                          facecolor=MAJORITY_MID, edgecolor=MAJORITY_MID,
                          linewidth=0, zorder=2)
    ax.add_patch(zk3)
    ax.text(10.95, 2.97, "zk-3", ha="center", va="center",
            fontsize=17, fontweight="bold", color="white")
    ax.text(10.95, 2.48, "follower",
            ha="center", va="center", fontsize=11, color="white")

    # Writes accepted
    wa = FancyBboxPatch((7.5, 0.65), 4.8, 1.35,
                         boxstyle="round,pad=0.18",
                         facecolor="white", edgecolor=MAJORITY_BORDER,
                         linewidth=1.8, zorder=2)
    ax.add_patch(wa)
    ax.text(9.9, 1.42, "writes accepted",
            ha="center", va="center",
            fontsize=12, fontweight="bold", color="#1B5E20")
    ax.text(9.9, 0.95, "3 of 5 — quorum intact",
            ha="center", va="center", fontsize=11, color="#666666")

    save(fig, "figG_topology.png")


# ===========================================================================
# FIGURE 3 — Timing Charts (slide 3 style)
# ===========================================================================
def fig_timing():
    fig, (ax_l, ax_r) = plt.subplots(
        1, 2, figsize=(12, 5.5),
        gridspec_kw={"width_ratios": [2.8, 1]},
    )

    # ── Legend (top of figure) ────────────────────────────────────────────
    legend_patches = [
        mpatches.Patch(color=BAR_D, label="First Failed Write (Minority)"),
        mpatches.Patch(color=BAR_B, label="New Leader Elected (Majority)"),
        mpatches.Patch(color=BAR_C, label="First Write (Majority)"),
        mpatches.Patch(color=BAR_F, label="First Write After Heal"),
    ]
    fig.legend(handles=legend_patches, loc="upper center",
               ncol=4, fontsize=10, framealpha=0.95,
               bbox_to_anchor=(0.5, 1.04),
               handlelength=1.5, handleheight=1.0)

    # ── Left: partition failure timing ─────────────────────────────────────
    left_vals   = [D_mean, B_mean, B_mean]  # D, B, C (C≈B inferred)
    left_colors = [BAR_D, BAR_B, BAR_C]
    left_labels = [
        "First Failed Write\n(Minority Side)",
        "New Leader Elected\n(Majority Side)",
        "First Write\n(Majority Side)",
    ]
    all_left_runs = [D_A, B_A, B_A]

    bars_l = ax_l.bar(range(3), left_vals, color=left_colors,
                       width=0.48, zorder=3)

    # Value labels
    for bar, val in zip(bars_l, left_vals):
        ax_l.text(bar.get_x() + bar.get_width() / 2,
                  val + 0.12, f"{val:.3f}s",
                  ha="center", va="bottom",
                  fontsize=12, fontweight="bold", color="#2C3E50")

    # Individual run dots
    offsets_dot = [-0.10, 0, 0.10]
    for i, (run_vals, c) in enumerate(zip(all_left_runs, left_colors)):
        for j, v in enumerate(run_vals):
            ax_l.scatter(i + offsets_dot[j], v,
                         color="white", edgecolors=c,
                         s=55, zorder=5, linewidths=1.8)

    ax_l.set_xticks(range(3))
    ax_l.set_xticklabels(left_labels, fontsize=10.5)
    ax_l.set_ylabel("Time since injection (s)", fontsize=11)
    ax_l.set_title("Partition failure — time since injection (s)",
                   fontsize=12, fontweight="bold", pad=10)
    ax_l.set_ylim(0, 8)
    ax_l.yaxis.set_major_locator(plt.MultipleLocator(1))
    ax_l.yaxis.set_minor_locator(plt.MultipleLocator(0.5))

    # Note: C = B (inferred)
    ax_l.text(0.98, 0.03,
              "* First Write (Majority) inferred = New Leader Elected\n"
              "  (write available the moment a leader exists on majority side)",
              transform=ax_l.transAxes, ha="right", va="bottom",
              fontsize=8, color="#7F8C8D", style="italic")

    # ── Right: partition heal timing ──────────────────────────────────────
    # Show all 3 runs individually (high variance: 33.3, 0.5, 33.2)
    run_labels_r = ["Run 1\n33.3s", "Run 2\n0.5s", "Run 3\n33.2s"]
    run_colors_r = [BAR_F, BAR_F, BAR_F]
    alphas       = [0.9, 0.4, 0.9]   # run 2 muted as outlier

    bars_r = []
    for i, (v, a) in enumerate(zip(F_E, alphas)):
        b = ax_r.bar(i, v, color=BAR_F, width=0.5,
                     alpha=a, zorder=3)
        bars_r.append(b)
        ax_r.text(i, v + 0.6, f"{v}s",
                  ha="center", va="bottom",
                  fontsize=11, fontweight="bold", color="#2C3E50")

    ax_r.set_xticks(range(3))
    ax_r.set_xticklabels(run_labels_r, fontsize=10)
    ax_r.set_ylabel("Time since removal (s)", fontsize=11)
    ax_r.set_title("Partition heal — time since removal (s)",
                   fontsize=12, fontweight="bold", pad=10)
    ax_r.set_ylim(0, 42)
    ax_r.yaxis.set_major_locator(plt.MultipleLocator(5))

    # Annotate run 2 outlier
    ax_r.annotate(
        "Run 2: minority nodes\nalready rejoined during\n75s monitoring window",
        xy=(1, 0.5), xytext=(1.65, 12),
        fontsize=8, color="#7F8C8D", ha="center",
        arrowprops=dict(arrowstyle="->", color="#AAB7B8", lw=1.2),
    )

    # Mean line (runs 1 & 3)
    r1r3_mean = (F_E[0] + F_E[2]) / 2
    ax_r.axhline(r1r3_mean, color=BAR_F, linewidth=1.4,
                 linestyle="--", alpha=0.6, zorder=4)
    ax_r.text(2.48, r1r3_mean + 0.8, f"r1+r3 mean\n{r1r3_mean:.1f}s",
              fontsize=8, color=BAR_F, ha="right", va="bottom")

    fig.tight_layout()
    save(fig, "figG_timing.png")


# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Generating Experiment G slide-style graphs ...")
    fig_kill_leader()
    fig_topology()
    fig_timing()
    print("\nAll graphs saved to graphs/:")
    print("  figG_kill_leader.png — election latency + phase breakdown (slide 1 style)")
    print("  figG_topology.png    — partition topology diagram (slide 2 style)")
    print("  figG_timing.png      — partition failure/heal timing charts (slide 3 style)")
