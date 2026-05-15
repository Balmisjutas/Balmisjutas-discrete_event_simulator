# Discrete Event Simulator

Simulates an IoT gateway with Poisson-distributed message arrivals and exponential service times across M/M/c/K queueing systems. Tracks occupancy metrics, latency, and system behavior under various queue configurations.

## Files

| File | Purpose |
|------|---------|
| engine.py | Core simulation engine and event loop |
| gateway.py | IoT gateway with queue and service processing |
| client.py | Message source generating arrivals |
| server.py | Service processor handling queue drains |
| queue.py | Priority queue implementation |
| scheduler.py | Event scheduling and timestep management |
| event.py | Event type definitions |
| message.py | Message payload structure |
| run_scenarios.py | Batch runner for M/M/c/K parameter sweeps |
| plot_results.py | Visualization of simulation results |

## How to Run

Single run with specified parameters:
```bash
python engine.py -l 4 -m 8
```

Batch runner for multiple configurations:
```bash
python run_scenarios.py
```

## Parameters

- `-l` Arrival rate (messages/second)
- `-m` Service rate (messages/second)
- `-s` Number of servers (default: 1)
- `-q` Queue capacity, waiting room only (default: 999999)
- `-t` Simulation duration in seconds (default: 100)
- `-v` Verbose event trace output