import contextlib
import io
import json
import math
import scipy.stats

from engine import Engine

SCENARIOS = [
    {"name": "M/M/1",   "n_clients": 1, "mu_rate": 8, "n_servers": 1, "queue_capacity": 999999},
    {"name": "M/M/1/4", "n_clients": 1, "mu_rate": 8, "n_servers": 1, "queue_capacity": 3},
    {"name": "M/M/1/8", "n_clients": 1, "mu_rate": 8, "n_servers": 1, "queue_capacity": 7},
    {"name": "M/M/3/8", "n_clients": 1, "mu_rate": 8, "n_servers": 3, "queue_capacity": 5},
]

LAMBDA_VALUES = [4, 6, 8, 12]
N_RUNS = 100
SIM_TIME = 1000
METRICS = ["dropped", "avg_wait_time", "avg_system_time", "avg_service_time", "avg_n_system", "avg_n_queue"]


# 95% confidence interval using t-distribution,
# correct for small sample sizes (N_RUNS=5).
def ci(values):
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    sem = math.sqrt(variance) / math.sqrt(n)
    lo, hi = scipy.stats.t.interval(0.95, df=n - 1, loc=mean, scale=sem)
    return mean, lo, hi


def run_all():
    engine = Engine()
    results = {}

    # Opened once to avoid repeated open/close per run.
    log_file = open("simulation_trace.log", "w")

    for scenario in SCENARIOS:
        name = scenario["name"]
        results[name] = {}
        print(f"\n{'='*70}")
        print(f"Scenario: {name}  (servers={scenario['n_servers']}, queue_cap={scenario['queue_capacity']})")
        print(f"{'='*70}")

        header = f"{'lambda':>8}  {'metric':<20}  {'mean':>12}  {'CI lower':>12}  {'CI upper':>12}"
        print(header)
        print("-" * len(header))

        for lam in LAMBDA_VALUES:
            # Collect all runs before computing stats,
            # so ci() sees the full sample at once.
            runs = []
            for run_idx in range(N_RUNS):
                buf = io.StringIO()
                # Drop messages print inside gateway.py, which we
                # can't silence there. Redirect stdout per run so
                # they go to the log file instead of the terminal.
                with contextlib.redirect_stdout(buf):
                    m = engine.run_simulation(
                        n_clients=scenario["n_clients"],
                        lambda_rate=lam,
                        mu_rate=scenario["mu_rate"],
                        n_servers=scenario["n_servers"],
                        queue_capacity=scenario["queue_capacity"],
                        sim_time=SIM_TIME,
                        verbose=False,
                    )
                captured = buf.getvalue()
                if captured:
                    log_file.write(f"[{name}  λ={lam}  run={run_idx + 1}]\n")
                    log_file.write(captured)
                    log_file.write("\n")
                runs.append({k: m[k] for k in METRICS})

            results[name][str(lam)] = runs

            first = True
            for metric in METRICS:
                values = [r[metric] for r in runs]
                mean, lo, hi = ci(values)
                lam_col = f"λ={lam}" if first else ""
                print(f"{lam_col:>8}  {metric:<20}  {mean:>12.4f}  {lo:>12.4f}  {hi:>12.4f}")
                first = False
            print()

    log_file.close()

    with open("simulation_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("Results saved to simulation_results.json")
    print("Trace (drop messages) saved to simulation_trace.log")


if __name__ == "__main__":
    run_all()
