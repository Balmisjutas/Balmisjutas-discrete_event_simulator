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

    def get_idle_server(self):
        for server in self.servers:
            if not server.get_is_busy():
                return server
        return None

    def receive_message(self, message, current_time, scheduler):
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

    def get_metrics(self):
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

        return {
            "received": self.total_received,
            "dropped": self.total_dropped,
            "drop_rate": drop_rate,
            "served": self.total_served,
            "avg_wait_time": avg_wait,
            "avg_system_time": avg_system,
            "avg_service_time": avg_service_time,  # across all servers
            "per_server": per_server                # breakdown per server
        }