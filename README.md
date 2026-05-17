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

**Important:** Our simulator uses a flag system to set parameters manually from the runner file. Hence it is important you first run the `-h` flag to see all available options like this:
    python engine.py -h

### Parameters

| Flag | Description | Default |
|------|-------------|---------|
| `-h` | Show help menu with all options and examples | none |
| `-l` | Arrival rate lambda (msg/s) | required |
| `-m` | Service rate mu (msg/s) | required |
| `-n` | Number of client sources | 1 |
| `-s` | Number of servers | 1 |
| `-q` | Queue capacity (waiting room only) | 999999 |
| `-t` | Simulation time in seconds | 100 |
| `-v` | Print full event trace | off |

### Examples

Single run (M/M/1 infinite queue):
    python engine.py -l 4 -m 8

Single run (M/M/1/4 finite queue):
    python engine.py -l 4 -m 8 -q 3 -t 1000

Batch runner for all 4 scenarios:
    python run_scenarios.py