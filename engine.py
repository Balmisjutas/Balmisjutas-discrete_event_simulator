from message import Message
from event import Event, EventType
from scheduler import Scheduler
from queue import Queue
from server import Server
from client import Client
from gateway import Gateway

class Engine:
    def __init__(self):
        self.trace_id = 1

    def print_trace_header(self):
        print(f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<7} {'msgID':<5}")
        print("-" * 50)

    def generate_trace(self, event):
        message = event.get_message()
        time = event.get_event_time()
        event_type = event.get_event_type().value
        source = message.get_source()
        destination = message.get_destination()
        msg_id = message.get_message_id()

        if event.get_event_type() == EventType.SEND_MSG:
            node = f"C{source}"
        else:
            node = f"S{source}"

        print(f"{time:<8.3f} {node:<5} {event_type:<6} {source:<7} {destination:<7} {msg_id:<5}")

    def test_client(self):
        print(" Testing Client ")
        scheduler = Scheduler()
        clients = []
        lambda_rates = [0.5, 1.0, 5.0]

        for i in range(3):
            clients.append(Client(i+1, lambda_rate=lambda_rates[i]))

        for client in clients:
            client.send_message(current_time=0.0, scheduler=scheduler)

        self.print_trace_header()
        last_time = 0.0
        for _ in range(3):
            event = scheduler.get_event()
            last_time = event.get_event_time()
            self.generate_trace(event)

        for client in clients:
            client.send_message(current_time=last_time, scheduler=scheduler)

        for _ in range(3):
            event = scheduler.get_event()
            self.generate_trace(event)

    def test_gateway(self):
        print("=== Testing Gateway ===")

        scheduler = Scheduler()
        queue = Queue(capacity=2)
        gateway = Gateway(queue, n_servers=3, mu_rate=2)

        client = Client(2, lambda_rate=1)
        client.send_message(0.0, scheduler)

        self.print_trace_header()

        time_limit = 10.0
        stop_sending = False

        while not scheduler.is_empty():
            event = scheduler.get_event()
            current_time = event.get_event_time()

            if current_time > time_limit:
                stop_sending = True

            self.generate_trace(event)

            if event.get_event_type() == EventType.SEND_MSG:
                event.get_message().set_send_time(current_time)
                if not stop_sending:
                    client.send_message(current_time, scheduler)
                recv_event = Event(current_time + 1, EventType.RECV_MSG, event.get_message())
                scheduler.add_event(recv_event)
            elif event.get_event_type() == EventType.RECV_MSG:
                gateway.receive_message(event.get_message(), current_time, scheduler)
            elif event.get_event_type() == EventType.MSG_DEPT:
                gateway.process_departure(event.get_message(), current_time, scheduler)

        print()

    def run_simulation(self, n_clients, lambda_rate, mu_rate, n_servers, queue_capacity, sim_time, verbose=False):
        Message._id_counter = 1
        scheduler = Scheduler()
        queue = Queue(capacity=queue_capacity)
        gateway = Gateway(queue, n_servers, mu_rate)

        # Create all clients and schedule their first message
        clients = [Client(i+1, lambda_rate) for i in range(n_clients)]
        for client in clients:
            client.send_message(0.0, scheduler)

        if verbose:
            self.print_trace_header()

        stop_sending = False

        while not scheduler.is_empty():
            event = scheduler.get_event()
            current_time = event.get_event_time()

            if current_time > sim_time:
                stop_sending = True

            if verbose:
                self.generate_trace(event)

            if event.get_event_type() == EventType.SEND_MSG:
                event.get_message().set_send_time(current_time)
                if not stop_sending:
                    # Find which client sent this message and schedule its next one
                    src_id = int(event.get_message().get_source())
                    sender = next(c for c in clients if int(c.client_id) == src_id)
                    sender.send_message(current_time, scheduler)
                recv_event = Event(current_time + 1, EventType.RECV_MSG, event.get_message())
                scheduler.add_event(recv_event)
            elif event.get_event_type() == EventType.RECV_MSG:
                gateway.receive_message(event.get_message(), current_time, scheduler)
            elif event.get_event_type() == EventType.MSG_DEPT:
                gateway.process_departure(event.get_message(), current_time, scheduler)

        metrics = gateway.get_metrics()
        if verbose:
            print(f"\n--- Results (λ={lambda_rate}, μ={mu_rate}, servers={n_servers}, cap={queue_capacity}) ---")
            print(f"  Received:          {metrics['received']}")
            print(f"  Served:            {metrics['served']}")
            print(f"  Dropped:           {metrics['dropped']}")
            print(f"  Drop rate:         {metrics['drop_rate']:.4f}")
            print(f"  Avg wait time:     {metrics['avg_wait_time']:.4f}s")
            print(f"  Avg system time:   {metrics['avg_system_time']:.4f}s")
            print(f"  Avg service time:  {metrics['avg_service_time']:.4f}s")
            print(f"  Per server:")
            for sid, data in metrics['per_server'].items():
                print(f"    Server {sid}: served={data['messages_served']}, avg_service={data['avg_service_time']:.4f}s")

        return metrics

    def run_tests(self):
        # self.test_client()
        # self.test_gateway()
        self.run_simulation(n_clients=2, lambda_rate=6, mu_rate=8, n_servers=2, queue_capacity=4, sim_time=30, verbose=True)
        
        # print("\n=== M/M/1 (no drops expected) ===")
        # self.run_simulation(n_clients=1, lambda_rate=4, mu_rate=8, n_servers=1, queue_capacity=100000, sim_time=30, verbose=True)

        # print("\n=== M/M/1/4 (drops expected) ===")
        # self.run_simulation(n_clients=2, lambda_rate=6, mu_rate=8, n_servers=1, queue_capacity=4, sim_time=30, verbose=True)

        # print("\n=== M/M/1/8 (fewer drops) ===")
        # self.run_simulation(n_clients=2, lambda_rate=6, mu_rate=8, n_servers=1, queue_capacity=8, sim_time=30, verbose=True)

        # print("\n=== M/M/3/8 (3 servers, fewer drops) ===")
        # self.run_simulation(n_clients=2, lambda_rate=6, mu_rate=8, n_servers=3, queue_capacity=8, sim_time=30, verbose=True)

if __name__ == "__main__":
    engine = Engine()
    engine.run_tests()