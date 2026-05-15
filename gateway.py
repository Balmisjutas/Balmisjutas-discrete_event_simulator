from event import Event, EventType
from server import Server

class Gateway:
    def __init__(self, queue, n_servers, mu_rate):
        self.queue = queue
        self.servers = [Server(str(i+1), mu_rate) for i in range(n_servers)]
        self.total_received = 0
        self.total_dropped = 0
        self.total_served = 0
        self.total_wait_time = 0.0
        self.total_system_time = 0.0
        self.last_event_time = 0.0
        self.area_n_system = 0.0
        self.area_n_queue = 0.0

    def _update_areas(self, current_time):
        elapsed = current_time - self.last_event_time
        queue_size = self.queue.get_size()
        busy = sum(1 for s in self.servers if s.get_is_busy())
        # integrate the current occupancy over the time slice since the last event
        # so that the time-weighted average is correct regardless of event spacing
        self.area_n_system += elapsed * (busy + queue_size)
        self.area_n_queue += elapsed * queue_size
        self.last_event_time = current_time

    def get_idle_server(self):
        for server in self.servers:
            if not server.get_is_busy():
                return server
        return None

    def receive_message(self, message, current_time, scheduler):
        self._update_areas(current_time)
        self.total_received += 1
        message.set_arrival_time(current_time)
        server = self.get_idle_server()
        if server:
            server.start_service(message, current_time, scheduler)
        else:
            accepted = self.queue.enqueue(message)
            if not accepted:
                self.total_dropped += 1
                print(f"  *** Message {message.get_message_id()} DROPPED - queue full! ***")

    def process_departure(self, message, current_time, scheduler):
        self._update_areas(current_time)
        server_id = message.get_source()
        server = next(s for s in self.servers if s.get_server_id() == server_id)
        server.finish_service()
        self.total_served += 1
        self.total_wait_time += message.get_service_start_time() - message.get_arrival_time()
        if message.get_send_time() is not None:
            self.total_system_time += current_time - message.get_send_time()
        if not self.queue.is_empty():
            next_msg = self.queue.dequeue()
            server.start_service(next_msg, current_time, scheduler)

    def get_metrics(self, sim_time):
        avg_wait = self.total_wait_time / self.total_served if self.total_served > 0 else 0
        avg_system = self.total_system_time / self.total_served if self.total_served > 0 else 0
        drop_rate = self.total_dropped / self.total_received if self.total_received > 0 else 0

        # aggregate service time across all servers
        total_all_service_time = sum(s.get_total_service_time() for s in self.servers)
        total_all_served = sum(s.get_total_messages_served() for s in self.servers)
        avg_service_time = total_all_service_time / total_all_served if total_all_served > 0 else 0

        # per server breakdown
        per_server = {
            s.get_server_id(): {
                "messages_served": s.get_total_messages_served(),
                "avg_service_time": s.get_avg_service_time()
            }
            for s in self.servers
        }

        # divide by sim_time rather than message count because these are
        # time-averaged quantities, not per-message averages
        avg_n_system = self.area_n_system / sim_time if sim_time > 0 else 0
        avg_n_queue = self.area_n_queue / sim_time if sim_time > 0 else 0

        return {
            "received": self.total_received,
            "dropped": self.total_dropped,
            "avg_n_system": avg_n_system,
            "avg_n_queue": avg_n_queue,
            "drop_rate": drop_rate,
            "served": self.total_served,
            "avg_wait_time": avg_wait,
            "avg_system_time": avg_system,
            "avg_service_time": avg_service_time,  # across all servers
            "per_server": per_server                # breakdown per server
        }