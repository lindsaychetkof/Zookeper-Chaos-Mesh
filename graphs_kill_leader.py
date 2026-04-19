#!/usr/bin/env python3
"""
graphs_kill_leader.py
=====================
Focused kill-leader presentation graphs derived directly from log data.
Run: python graphs_kill_leader.py
Output: graphs/kl_*.png
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker
import numpy as np

os.makedirs("graphs", exist_ok=True)

# ─── Style ────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family":        "DejaVu Sans",
    "font.size":          11,
    "axes.titlesize":     13,
    "axes.titleweight":   "bold",
    "axes.labelsize":     11,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "figure.facecolor":   "white",
    "axes.facecolor":     "#F8F9FA",
    "axes.grid":          True,
    "grid.color":         "white",
    "grid.linewidth":     1.0,
    "grid.alpha":         0.9,
    "figure.dpi":         150,
    "savefig.bbox":       "tight",
    "savefig.facecolor":  "white",
})

RED    = "#E74C3C"
GREEN  = "#27AE60"
BLUE   = "#2980B9"
ORANGE = "#E67E22"
GREY   = "#95A5A6"
DARK   = "#2C3E50"
AMBER  = "#F39C12"

def save(fig, name):
    path = os.path.join("graphs", name)
    fig.savefig(path)
    plt.close(fig)
    print(f"  Saved {path}")


# ─── Raw data from logs ───────────────────────────────────────────────────────

# V1 (5s polling, 5 runs): no election latency captured, only recovery
v1_recovery = [64.1, 67.8, 62.5, 67.3, 67.7]   # from log summary lines

# V2 (1s parallel polling, 3 runs): both election latency and recovery
v2 = [
    # (run, election_latency_s, killed_pod_rejoin_s, fault_removed_s, recovery_s)
    (1, 3.791, 10.08,  62.12, 64.2),
    (2, 3.359, 15.96,  60.61, 64.1),
    (3, 3.650, 13.26,  63.33, 67.7),
]
v2_runs        = [r[0] for r in v2]
v2_elec        = [r[1] for r in v2]
v2_rejoin      = [r[2] for r in v2]
v2_fault_rem   = [r[3] for r in v2]
v2_recovery    = [r[4] for r in v2]

all_recovery   = v1_recovery + v2_recovery       # 8 total runs

# Client workload impact (from workload_kill_leader.log)
WL_LAST_OK_S   = 0.0          # relative zero: last OK write (18:13:56.157)
WL_ERR1_S      = 1.046        # first ERROR (18:13:57.203)
WL_ERR2_S      = 2.452        # session expired error (18:13:58.609)
WL_NEXT_OK_S   = 54.346       # reconnect OK (18:14:50.503)
WL_CLIENT_DOWN = WL_NEXT_OK_S - WL_LAST_OK_S   # ~54.3s


# ─── Fig 1: Event Timeline (3 v2 runs side-by-side) ──────────────────────────
def fig_timeline():
    fig, ax = plt.subplots(figsize=(13, 5.5))

    run_colors = [RED, BLUE, ORANGE]
    run_labels = ["Run 1  (leader: zk-2 → zk-1)",
                  "Run 2  (leader: zk-1 → zk-2)",
                  "Run 3  (leader: zk-2 → zk-1)"]

    y_positions = [2.0, 1.0, 0.0]
    bar_h = 0.28

    for i, (y, color, lbl) in enumerate(zip(y_positions, run_colors, run_labels)):
        t_elec   = v2_elec[i]
        t_rejoin = v2_rejoin[i]
        t_rem    = v2_fault_rem[i]
        t_rec    = v2_recovery[i]

        # Phase 1: fault injected → new leader elected
        ax.barh(y, t_elec, height=bar_h, left=0,
                color=RED, alpha=0.85, zorder=3)
        # Phase 2: election done → killed pod rejoins as follower
        ax.barh(y, t_rejoin - t_elec, height=bar_h, left=t_elec,
                color=AMBER, alpha=0.75, zorder=3)
        # Phase 3: pod rejoined → fault removed
        ax.barh(y, t_rem - t_rejoin, height=bar_h, left=t_rejoin,
                color=GREEN, alpha=0.65, zorder=3)
        # Phase 4: fault removed → recovery confirmed
        ax.barh(y, t_rec - t_rem, height=bar_h, left=t_rem,
                color=BLUE, alpha=0.85, zorder=3)

        # Event markers
        for t, symbol, color_m in [
            (0,       "▼", DARK),
            (t_elec,  "★", GREEN),
            (t_rejoin,"●", AMBER),
            (t_rem,   "◆", DARK),
            (t_rec,   "✓", BLUE),
        ]:
            ax.text(t, y + bar_h / 2 + 0.06, symbol,
                    ha="center", va="bottom", fontsize=10, color=color_m, zorder=5)

        # Latency annotations
        ax.text(t_elec / 2, y, f"{t_elec:.2f}s",
                ha="center", va="center", fontsize=9, color="white", fontweight="bold")
        ax.text(t_rec + 0.5, y, f"total {t_rec:.1f}s",
                ha="left", va="center", fontsize=9, color=DARK)

        # Run label on left
        ax.text(-2, y, f"Run {i+1}", ha="right", va="center",
                fontsize=10, color=color, fontweight="bold")

    # Legend for phases
    phase_patches = [
        mpatches.Patch(color=RED,   alpha=0.85, label="Phase 1 — Fault injected → leader elected"),
        mpatches.Patch(color=AMBER, alpha=0.75, label="Phase 2 — Killed pod restarts & rejoins"),
        mpatches.Patch(color=GREEN, alpha=0.65, label="Phase 3 — Cluster stable (fault still active)"),
        mpatches.Patch(color=BLUE,  alpha=0.85, label="Phase 4 — Fault removed → confirmed healthy"),
    ]
    ax.legend(handles=phase_patches, loc="upper right", fontsize=9, framealpha=0.92)

    # Symbol legend
    symbol_lines = "  ▼ fault injected   ★ new leader elected   ● pod rejoined   ◆ fault removed   ✓ recovered"
    ax.text(0.01, -0.09, symbol_lines, transform=ax.transAxes,
            fontsize=8.5, color=DARK, style="italic")

    ax.set_xlim(-8, 72)
    ax.set_ylim(-0.45, 2.6)
    ax.set_xlabel("Seconds from fault injection (leader pod killed)")
    ax.set_yticks([])
    ax.set_title("Fig 1 — Kill-Leader Event Timeline  (v2, 1-second polling, 3 runs)\n"
                 "3-node ZooKeeper ensemble — Chaos Mesh PodChaos")

    ax.axvline(0, color=DARK, linewidth=1.5, linestyle="--", alpha=0.4, zorder=2)
    ax.text(0.3, 2.52, "Kill\ninjected", fontsize=8, color=DARK, va="top")

    fig.tight_layout()
    save(fig, "kl_fig1_timeline.png")


# ─── Fig 2: Election Latency detail ──────────────────────────────────────────
def fig_election_latency():
    fig, axes = plt.subplots(1, 2, figsize=(12, 5),
                             gridspec_kw={"width_ratios": [1.4, 1]})

    # Left: bar chart per run
    ax = axes[0]
    xs = np.arange(3)
    colors = [RED, BLUE, ORANGE]
    bars = ax.bar(xs, v2_elec, color=colors, width=0.5,
                  zorder=3, edgecolor="white", linewidth=0.5)

    mean_elec = np.mean(v2_elec)
    std_elec  = np.std(v2_elec)

    ax.axhline(mean_elec, color=DARK, linewidth=1.8, linestyle="--",
               zorder=4, label=f"Mean = {mean_elec:.3f}s")
    ax.fill_between([-0.4, 2.4], mean_elec - std_elec, mean_elec + std_elec,
                    color=DARK, alpha=0.08, zorder=2,
                    label=f"±1 SD  (SD = {std_elec:.3f}s)")

    for bar, val in zip(bars, v2_elec):
        ax.text(bar.get_x() + bar.get_width() / 2, val + 0.05,
                f"{val:.3f}s", ha="center", va="bottom",
                fontsize=10.5, fontweight="bold", color=DARK)

    ax.set_xticks(xs)
    ax.set_xticklabels(["Run 1\n(zk-2 killed)", "Run 2\n(zk-1 killed)", "Run 3\n(zk-2 killed)"])
    ax.set_ylabel("Election latency (seconds)")
    ax.set_ylim(0, 6)
    ax.set_title("Leader Election Latency — per run")
    ax.legend(fontsize=10, loc="upper right")
    ax.text(0.03, 0.97,
            "Time from fault injection\nto new leader confirmed\n(1-second polling resolution)",
            transform=ax.transAxes, fontsize=9, color=GREY, va="top", style="italic")

    # Right: summary stats panel
    ax2 = axes[1]
    ax2.axis("off")

    stats = [
        ("Min",    f"{min(v2_elec):.3f} s",   GREEN),
        ("Mean",   f"{mean_elec:.3f} s",       DARK),
        ("Max",    f"{max(v2_elec):.3f} s",    RED),
        ("Std Dev",f"{std_elec:.3f} s",        GREY),
        ("Runs",   "3 / 3  (100% success)",    GREEN),
        ("Quorum\nlost?", "Never  (2/3 survive)", GREEN),
    ]

    ax2.set_xlim(0, 1)
    ax2.set_ylim(0, 1)

    # Draw a card-like box
    card = mpatches.FancyBboxPatch((0.05, 0.05), 0.9, 0.9,
                                    boxstyle="round,pad=0.02",
                                    facecolor="#EBF5FB", edgecolor=BLUE,
                                    linewidth=2, zorder=1)
    ax2.add_patch(card)

    ax2.text(0.5, 0.93, "Election Latency Summary",
             ha="center", va="top", fontsize=11, fontweight="bold", color=DARK)

    row_y = 0.80
    for label, val, color in stats:
        ax2.text(0.18, row_y, label + ":", ha="right", va="center",
                 fontsize=10.5, color=GREY)
        ax2.text(0.22, row_y, val, ha="left", va="center",
                 fontsize=10.5, fontweight="bold", color=color)
        row_y -= 0.13

    fig.suptitle("Fig 2 — ZAB Leader Election: New Leader Elected in ~3.6 Seconds",
                 fontsize=13, fontweight="bold", y=1.01)
    fig.tight_layout()
    save(fig, "kl_fig2_election_latency.png")


# ─── Fig 3: Phase breakdown (stacked bar, all 8 runs) ────────────────────────
def fig_phase_breakdown():
    """
    Shows recovery_seconds across all 8 runs, broken into components for v2:
      - election phase (~3-4s)
      - pod-rejoin phase (election end → pod back as follower)
      - stable-under-fault phase (rejoin → fault removed)
      - confirmation phase (fault removed → confirmed healthy)
    V1 runs shown as single bars (no phase breakdown possible at 5s resolution).
    """
    fig, ax = plt.subplots(figsize=(13, 5.5))

    # V1 bars (runs 1-5) as solid grey
    for i, rec in enumerate(v1_recovery):
        ax.bar(i, rec, color=GREY, width=0.55, alpha=0.75, zorder=3,
               edgecolor="white", linewidth=0.5)
        ax.text(i, rec + 0.8, f"{rec}s", ha="center", va="bottom",
                fontsize=9, color=GREY, fontweight="bold")

    # V2 bars (runs 6-8) as stacked phases
    v2_x = [5, 6, 7]
    phase_colors = [RED, AMBER, GREEN, BLUE]
    phase_labels = ["Phase 1: Election (~4s)",
                    "Phase 2: Pod restart & rejoin",
                    "Phase 3: Stable under fault",
                    "Phase 4: Post-removal confirm"]

    for idx, (x, run_data) in enumerate(zip(v2_x, v2)):
        _, t_elec, t_rejoin, t_rem, t_rec = run_data
        phases = [
            t_elec,
            t_rejoin - t_elec,
            t_rem    - t_rejoin,
            t_rec    - t_rem,
        ]
        bottom = 0
        for p_val, p_color in zip(phases, phase_colors):
            ax.bar(x, p_val, bottom=bottom, color=p_color, width=0.55,
                   zorder=3, edgecolor="white", linewidth=0.5, alpha=0.88)
            if p_val > 2:  # only label if segment is large enough
                ax.text(x, bottom + p_val / 2, f"{p_val:.1f}s",
                        ha="center", va="center", fontsize=8.5,
                        color="white" if p_color != AMBER else DARK,
                        fontweight="bold")
            bottom += p_val
        ax.text(x, t_rec + 0.8, f"{t_rec}s", ha="center", va="bottom",
                fontsize=9, color=DARK, fontweight="bold")

    # Divider between v1 and v2
    ax.axvline(4.5, color=DARK, linewidth=1.2, linestyle=":", alpha=0.5, zorder=5)
    ax.text(4.52, 5, "v1 →  v2", fontsize=8.5, color=DARK, va="bottom")

    # X-axis labels
    xticks = list(range(8))
    xlabels = [f"v1\nRun {i+1}" for i in range(5)] + [f"v2\nRun {i+1}" for i in range(3)]
    ax.set_xticks(xticks)
    ax.set_xticklabels(xlabels, fontsize=9.5)

    # Legend
    patches = [mpatches.Patch(color=GREY,  alpha=0.75, label="v1 — total recovery (5s polling, no phase detail)")]
    for lbl, c in zip(phase_labels, phase_colors):
        patches.append(mpatches.Patch(color=c, alpha=0.88, label=lbl))
    ax.legend(handles=patches, loc="upper right", fontsize=8.5, framealpha=0.92)

    ax.set_ylabel("Seconds from fault injection")
    ax.set_ylim(0, 80)
    ax.set_title("Fig 3 — Recovery Breakdown: All 8 Kill-Leader Runs\n"
                 "v1 (5s polling) — single bar   |   v2 (1s polling) — stacked by phase")

    fig.tight_layout()
    save(fig, "kl_fig3_phase_breakdown.png")


# ─── Fig 4: Client impact (workload log) ─────────────────────────────────────
def fig_client_impact():
    """
    Recreates the client-visible timeline from workload_kill_leader.log.
    The log shows writes at ~0.5s intervals around a ~54s outage window.
    """
    fig, axes = plt.subplots(2, 1, figsize=(13, 6.5),
                              gridspec_kw={"height_ratios": [2, 1]})

    # ── Build op timeline from workload log ───────────────────────────────────
    # Times are relative to last successful op before failure (t=0)
    # Before failure: ops at -5.5s through 0s (11 successful ops at 0.5s each)
    # After reconnect: ops resume at +54.3s
    PRE_OPS  = 11   # ops 0–10 (the ones before the kill)
    POST_OPS = 7    # ops 11–17 shown in log after reconnect

    pre_times  = np.arange(PRE_OPS)  * (-0.526) + (-5.243)   # back-filled at 0.526s interval
    post_times = np.array([54.346, 54.882, 55.487, 56.009, 56.579, 57.106, 57.628])

    # Error window: 18:13:57.203 → 18:14:50.503  (from log)
    err_start = pre_times[-1] + 0.526    # right after last OK
    err_end   = post_times[0] - 0.526    # right before next OK
    err_mid   = (err_start + err_end) / 2

    ax = axes[0]

    # Shade fault/outage window
    ax.axvspan(err_start, err_end, alpha=0.12, color=RED,
               zorder=1, label=f"Client outage  (~{WL_CLIENT_DOWN:.0f}s)")

    # OK ops
    ax.scatter(pre_times, np.ones(len(pre_times)),
               color=GREEN, s=60, zorder=4, label="OK — write+read")
    ax.scatter(post_times, np.ones(len(post_times)),
               color=GREEN, s=60, zorder=4)

    # Error events
    ax.scatter([err_start, err_start + 1.4], [1, 1],
               color=RED, marker="X", s=200, zorder=5,
               label="ERROR — ConnectionLoss / Session Expired")

    # Annotations
    ax.annotate("Leader killed\n(fault injected)",
                xy=(err_start, 1), xytext=(err_start + 1, 1.28),
                arrowprops=dict(arrowstyle="->", color=RED, lw=1.5),
                fontsize=9.5, color=RED, ha="left")

    ax.annotate(f"New leader elected\n(~3.6s after kill)",
                xy=(err_start + 3.6, 1), xytext=(err_start + 3.6, 0.68),
                arrowprops=dict(arrowstyle="->", color=ORANGE, lw=1.5),
                fontsize=9.5, color=ORANGE, ha="center")
    ax.axvline(err_start + 3.6, color=ORANGE, linewidth=1.4, linestyle=":",
               zorder=4, alpha=0.8)

    ax.annotate("Client reconnects\n(ZK session re-established)",
                xy=(post_times[0], 1), xytext=(post_times[0] - 1, 1.28),
                arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.5),
                fontsize=9.5, color=GREEN, ha="right")

    # Outage duration arrow
    ax.annotate("", xy=(err_end, 0.78), xytext=(err_start, 0.78),
                arrowprops=dict(arrowstyle="<->", color=RED, lw=1.8))
    ax.text(err_mid, 0.74, f"Client outage: ~{WL_CLIENT_DOWN:.0f}s",
            ha="center", va="top", fontsize=10, color=RED, fontweight="bold")

    ax.set_xlim(pre_times[0] - 2, post_times[-1] + 3)
    ax.set_ylim(0.5, 1.5)
    ax.set_yticks([])
    ax.set_title("Fig 4 — Client-Visible Impact: ZooKeeper Write/Read Operations\n"
                 "Source: workload_kill_leader.log  (0.5s op interval, port-forward)")
    ax.legend(loc="upper right", fontsize=9.5, framealpha=0.92)

    # Key insight box
    ax.text(0.01, 0.97,
            "Election took ~3.6s — but client sees ~54s outage.\n"
            "Gap = ZooKeeper session expiry + client retry backoff.",
            transform=ax.transAxes, fontsize=9, color=DARK, va="top",
            style="italic",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#EBF5FB",
                      edgecolor=BLUE, alpha=0.9))

    # ── Bottom: gap explanation bar ───────────────────────────────────────────
    ax2 = axes[1]
    ax2.axis("off")

    ax2.set_xlim(0, 100)
    ax2.set_ylim(0, 1)

    # Draw a horizontal decomposition bar
    segments = [
        (0,   3.6,  RED,    "Election\n~3.6s"),
        (3.6, 30,   AMBER,  "Session timeout\n(ZK default ~30s)"),
        (30,  54.3, BLUE,   "Client retry &\nre-establishment"),
    ]
    for x0, x1, color, lbl in segments:
        ax2.barh(0.5, x1 - x0, left=x0, height=0.38,
                 color=color, alpha=0.82, zorder=3,
                 edgecolor="white", linewidth=0.8)
        ax2.text((x0 + x1) / 2, 0.5, lbl,
                 ha="center", va="center", fontsize=9,
                 color="white" if color != AMBER else DARK, fontweight="bold")

    ax2.text(54.3 / 2, 0.12,
             f"← Total client downtime: ~{WL_CLIENT_DOWN:.0f}s →",
             ha="center", va="bottom", fontsize=10, color=DARK, fontweight="bold")

    ax2.set_xlabel("Seconds from fault injection", fontsize=10, labelpad=2)
    # Manually add x-axis since ax is off
    for x in [0, 3.6, 10, 20, 30, 40, 54.3]:
        ax2.text(x, 0.04, f"{x:.0f}s", ha="center", va="bottom",
                 fontsize=8, color=DARK)

    fig.tight_layout()
    save(fig, "kl_fig4_client_impact.png")


# ─── Fig 5: Summary dashboard ─────────────────────────────────────────────────
def fig_summary():
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.suptitle("Kill-Leader Test — Summary Dashboard\n"
                 "3-node ZooKeeper on Kubernetes (Chaos Mesh PodChaos)",
                 fontsize=14, fontweight="bold")

    # ── TL: Election latency (v2 runs) ───────────────────────────────────────
    ax = axes[0][0]
    xs = np.arange(3)
    bars = ax.bar(xs, v2_elec, color=[RED, BLUE, ORANGE], width=0.5,
                  zorder=3, edgecolor="white")
    mean_e = np.mean(v2_elec)
    ax.axhline(mean_e, color=DARK, linewidth=2, linestyle="--",
               label=f"Mean = {mean_e:.2f}s")
    for bar, v in zip(bars, v2_elec):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.05,
                f"{v:.3f}s", ha="center", va="bottom", fontsize=10, fontweight="bold")
    ax.set_xticks(xs)
    ax.set_xticklabels(["Run 1", "Run 2", "Run 3"])
    ax.set_ylim(0, 7)
    ax.set_ylabel("Seconds")
    ax.set_title("Election Latency (v2, 1s polling)")
    ax.legend(fontsize=10)

    # ── TR: Recovery time across all 8 runs ──────────────────────────────────
    ax2 = axes[0][1]
    x8  = np.arange(8)
    c8  = [GREY]*5 + [RED, BLUE, ORANGE]
    lbl8 = [f"v1-{i+1}" for i in range(5)] + ["v2-1", "v2-2", "v2-3"]
    bars2 = ax2.bar(x8, all_recovery, color=c8, width=0.55,
                    zorder=3, edgecolor="white")
    ax2.axhline(np.mean(all_recovery), color=DARK, linewidth=1.8, linestyle="--",
                label=f"Mean = {np.mean(all_recovery):.1f}s")
    for bar, v in zip(bars2, all_recovery):
        ax2.text(bar.get_x() + bar.get_width() / 2, v + 0.5,
                 f"{v:.1f}", ha="center", va="bottom", fontsize=8.5)
    ax2.set_xticks(x8)
    ax2.set_xticklabels(lbl8, fontsize=8.5)
    ax2.set_ylim(0, 80)
    ax2.set_ylabel("Seconds")
    ax2.set_title("Total Recovery — All 8 Runs\n(v1: 5s polling  |  v2: 1s polling)")
    ax2.legend(fontsize=9)
    v1p = mpatches.Patch(color=GREY, alpha=0.75, label="v1 runs")
    v2p = mpatches.Patch(color=RED,  alpha=0.88, label="v2 runs")
    ax2.legend(handles=[v1p, v2p], fontsize=9)

    # ── BL: Phase breakdown for v2 (stacked) ─────────────────────────────────
    ax3 = axes[1][0]
    run_labels_short = ["Run 1", "Run 2", "Run 3"]
    phase_colors = [RED, AMBER, GREEN, BLUE]
    phase_names  = ["Election", "Pod rejoin", "Stable\n(fault active)", "Post-removal"]

    for idx in range(3):
        _, t_elec, t_rejoin, t_rem, t_rec = v2[idx]
        phases = [t_elec, t_rejoin - t_elec, t_rem - t_rejoin, t_rec - t_rem]
        bottom = 0
        for p, pc in zip(phases, phase_colors):
            ax3.bar(idx, p, bottom=bottom, color=pc, width=0.5,
                    zorder=3, edgecolor="white", alpha=0.9)
            if p > 1.5:
                ax3.text(idx, bottom + p / 2, f"{p:.1f}s",
                         ha="center", va="center", fontsize=9,
                         color="white" if pc != AMBER else DARK, fontweight="bold")
            bottom += p

    ax3.set_xticks(range(3))
    ax3.set_xticklabels(run_labels_short)
    ax3.set_ylabel("Seconds")
    ax3.set_ylim(0, 78)
    ax3.set_title("Recovery Phases — v2 Runs")
    patches_ph = [mpatches.Patch(color=c, alpha=0.9, label=n)
                  for c, n in zip(phase_colors, phase_names)]
    ax3.legend(handles=patches_ph, fontsize=8.5, loc="upper right")

    # ── BR: Key metrics text card ─────────────────────────────────────────────
    ax4 = axes[1][1]
    ax4.axis("off")

    card = mpatches.FancyBboxPatch((0.04, 0.04), 0.92, 0.92,
                                    boxstyle="round,pad=0.03",
                                    facecolor="#EBF5FB", edgecolor=BLUE,
                                    linewidth=2)
    ax4.add_patch(card)
    ax4.set_xlim(0, 1)
    ax4.set_ylim(0, 1)

    ax4.text(0.5, 0.93, "Key Results",
             ha="center", va="top", fontsize=13, fontweight="bold", color=DARK)

    metrics = [
        (GREEN, "✓", "New leader elected in", f"{np.mean(v2_elec):.2f}s  (±{np.std(v2_elec):.3f}s)"),
        (GREEN, "✓", "Quorum maintained in", "8 / 8 runs  (100%)"),
        (GREEN, "✓", "Leadership transferred in", "8 / 8 runs  (100%)"),
        (AMBER, "~", "Killed pod rejoin time", "10 – 16 s"),
        (BLUE,  "~", "Mean full recovery", f"{np.mean(all_recovery):.1f}s  (8 runs)"),
        (RED,   "!", "Client-visible downtime", f"~{WL_CLIENT_DOWN:.0f}s  (session expiry)"),
    ]

    row_y = 0.80
    for color, sym, label, val in metrics:
        ax4.text(0.08, row_y, sym, ha="center", va="center",
                 fontsize=13, color=color, fontweight="bold")
        ax4.text(0.15, row_y, label + ":", ha="left", va="center",
                 fontsize=9.5, color=GREY)
        ax4.text(0.15, row_y - 0.065, val, ha="left", va="center",
                 fontsize=10.5, color=color, fontweight="bold")
        row_y -= 0.145

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save(fig, "kl_fig5_summary_dashboard.png")


if __name__ == "__main__":
    print("Generating kill-leader presentation graphs ...")
    fig_timeline()
    fig_election_latency()
    fig_phase_breakdown()
    fig_client_impact()
    fig_summary()
    print("\nDone. Files in graphs/:")
    print("  kl_fig1_timeline.png")
    print("  kl_fig2_election_latency.png")
    print("  kl_fig3_phase_breakdown.png")
    print("  kl_fig4_client_impact.png")
    print("  kl_fig5_summary_dashboard.png")
