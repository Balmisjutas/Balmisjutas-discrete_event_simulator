import random
from event import Event, EventType

class Server:
    def __init__(self, server_id, mu_rate):
        self.server_id = str(server_id)
        self.mu_rate = mu_rate
        self.is_busy = False
        self.current_message = None
        self.total_messages_served = 0  # how many messages this server completed
        self.total_service_time = 0.0   # sum of all service durations for this server
        self.last_service_time = 0.0    # stores the current service duration until finish

    def get_server_id(self):
        return self.server_id
    def get_mu_rate(self):
        return self.mu_rate
    def get_is_busy(self):
        return self.is_busy
    def get_current_message(self):
        return self.current_message
    def get_total_messages_served(self):
        return self.total_messages_served
    def get_total_service_time(self):
        return self.total_service_time
    def get_avg_service_time(self):  # average service time for this server
        if self.total_messages_served > 0:
            return self.total_service_time / self.total_messages_served
        return 0

    def set_server_id(self, server_id):
        self.server_id = str(server_id)
    def set_mu_rate(self, mu_rate):
        self.mu_rate = mu_rate
    def set_is_busy(self, status):
        self.is_busy = status
    def set_current_message(self, message):
        self.current_message = message

    def get_service_time(self):
        return random.expovariate(self.mu_rate)

    def start_service(self, message, current_time, scheduler):
        if self.is_busy:
            return None
        self.current_message = message
        self.is_busy = True
        message.set_service_start_time(current_time)
        service_time = self.get_service_time()
        self.last_service_time = service_time   # store duration so finish_service can use it
        dept_time = current_time + service_time
        message.set_source(self.server_id)
        dept_event = Event(dept_time, EventType.MSG_DEPT, message)
        scheduler.add_event(dept_event)

    def finish_service(self):
        finished_message = self.current_message
        self.total_messages_served += 1                    # increment completed count
        self.total_service_time += self.last_service_time  # accumulate service duration
        self.current_message = None
        self.is_busy = False
        return finished_message

    def print_server(self):
        status = "Busy" if self.is_busy else "Idle"
        print(f"Server ID: {self.server_id}, Mu Rate: {self.mu_rate}, Status: {status}")