import json
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy import stats

LAMBDA_VALUES = [4, 6, 8, 12]
METRICS = ["dropped", "avg_wait_time", "avg_system_time", "avg_service_time", "avg_n_system", "avg_n_queue"]
METRIC_LABELS = {
    "received": "Received Messages",
    "dropped": "Dropped Messages",
    "avg_wait_time": "Avg Wait Time (s)",
    "avg_system_time": "Avg System Time (s)",
    "avg_service_time": "Avg Service Time (s)",
    "avg_n_system": "Avg Messages in System",
    "avg_n_queue": "Avg Messages in Queue",
}

with open("simulation_results.json") as f:
    data = json.load(f)

os.makedirs("plots", exist_ok=True)

count = 0
for scenario, lambda_data in data.items():
    for metric in METRICS:
        means, ci_lower, ci_upper = [], [], []

        for lam in LAMBDA_VALUES:
            runs = lambda_data[str(lam)]
            values = [r[metric] for r in runs]
            n = len(values)
            mean = np.mean(values)
            se = stats.sem(values)
            ci = se * stats.t.ppf(0.975, df=n - 1)
            means.append(mean)
            ci_lower.append(ci)
            ci_upper.append(ci)

        fig, ax = plt.subplots(figsize=(7, 5))
        ax.errorbar(
            LAMBDA_VALUES,
            means,
            # yerr accepts [lower_deltas, upper_deltas] so asymmetric CIs can
            # be expressed; here both lists are equal because a t-interval is
            # symmetric, but the two-list form keeps the call site consistent
            # with the general case where lower and upper margins differ.
            yerr=[ci_lower, ci_upper],
            fmt="o-",
            capsize=5,
            linewidth=1.5,
            markersize=6,
        )
        ax.set_xlabel("Lambda (arrival rate)")
        ax.set_ylabel(METRIC_LABELS[metric])
        ax.set_title(f"{scenario} - {METRIC_LABELS[metric]}")
        ax.set_xticks(LAMBDA_VALUES)
        ax.grid(True, linestyle="--", alpha=0.5)
        fig.tight_layout()

        filename = f"plots/{scenario.replace('/', '_')}_{metric}.png"
        # dpi=100 keeps file size reasonable for a report while still
        # rendering text and lines at a resolution that looks sharp on screen.
        fig.savefig(filename, dpi=100)
        # matplotlib accumulates figure objects in memory unless explicitly
        # closed; closing here prevents a leak across the 20-figure loop.
        plt.close(fig)
        count += 1

print(f"Generated {count} plots in plots/ directory")  # 4 scenarios × 5 metrics = 20
