from message import Message
from event import Event, EventType
from scheduler import Scheduler
from queue import Queue
from server import Server
from client import Client
from gateway import Gateway

class Engine:
    def __init__(self):
        self.trace_id = 1  # not used in table, but can be useful later

    #  TRACE HEADER 
    def print_trace_header(self):
        print(f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<7} {'msgID':<5}")
        print("-" * 50)

    # GENERATE TRACE (TABLE FORMAT)
    def generate_trace(self, event):
        message = event.get_message()
        time = event.get_event_time()
        event_type = event.get_event_type().value
        source = message.get_source()
        destination = message.get_destination()
        msg_id = message.get_message_id()

        # Clients own SEND events, servers own RECV and DEPT.
        # Prefixing avoids ambiguity since both use integer IDs.
        if event.get_event_type() == EventType.SEND_MSG:
            node = f"C{source}"
        else:
            node = f"S{source}"

        print(f"{time:<8.3f} {node:<5} {event_type:<6} {source:<7} {destination:<7} {msg_id:<5}")

    # TEST MESSAGE
    def test_message(self):
        print(" Testing Message ")
        msg1 = Message("Client", "Gateway")
        msg2 = Message("Client", "Server")

        msg1.print_message()
        msg2.print_message()
        print()

    # TEST EVENT 
    def test_event(self):
        print(" Testing Event ")
        msg = Message("Client", "Gateway")
        event = Event(0.5, EventType.SEND_MSG, msg)

        event.print_event()
        print()

    #  TEST SCHEDULER
    def test_scheduler(self):
        print(" Testing Scheduler ")
        scheduler = Scheduler()

        msg1 = Message("1", "0")
        msg2 = Message("1", "0")
        msg3 = Message("3", "0")

        e1 = Event(2.0, EventType.SEND_MSG, msg1)
        e2 = Event(1.0, EventType.RECV_MSG, msg2)
        e3 = Event(3.0, EventType.MSG_DEPT, msg3)

        scheduler.add_event(e1)
        scheduler.add_event(e2)
        scheduler.add_event(e3)

        scheduler.print_events()
        print()

    # TEST TRACE
    def test_trace(self):
        print(" Testing Trace Table ")

        self.print_trace_header()

        msg1 = Message("1", "0")
        msg2 = Message("3", "0")

        e1 = Event(1.202, EventType.SEND_MSG, msg1)
        e2 = Event(1.916, EventType.RECV_MSG, msg1)
        e3 = Event(2.320, EventType.SEND_MSG, msg2)
        e4 = Event(3.505, EventType.MSG_DEPT, msg1)

        self.generate_trace(e1)
        self.generate_trace(e2)
        self.generate_trace(e3)
        self.generate_trace(e4)

        print()

    # TEST QUEUE
    def test_queue(self):
        print(" Testing Queue ")

        queue = Queue(capacity=2)

        msg1 = Message("1", "0")
        msg2 = Message("2", "0")
        msg3 = Message("3", "0")

        print("Enqueue msg1:", queue.enqueue(msg1))
        print("Enqueue msg2:", queue.enqueue(msg2))
        print("Enqueue msg3:", queue.enqueue(msg3))

        queue.print_queue()

        print("Dequeue:", queue.dequeue().get_message_id())
        print("Queue size:", queue.get_size())
        print()

    # TEST SERVER
    def test_server(self):
        print(" Testing Server ")

        server = Server(1, mu_rate=2)  # average service time = 0.5

        msg1 = Message("1", "0")
        msg2 = Message("2", "0")

        # Start first service
        service_time = server.start_service(msg1)
        print("Started service for msg1, service time:", service_time)

        # Now we try to start second (but it should fail)
        result = server.start_service(msg2)
        print("Trying to start msg2:", result)

        server.print_server() 

        # Finish service
        finished = server.finish_service()
        print("Finished message:", finished.get_message_id())

        server.print_server() 
        print()
        
    # TEST CLIENT
    def test_client(self):
        print(" Testing Client ")
        scheduler = Scheduler()
        clients = []

        lambda_rates = [0.5, 1.0, 5.0]

        for i in range(3):
            clients.append(Client(i, lambda_rate=lambda_rates[i]))

        # Round 1: all clients start from t=0
        for client in clients:
            client.send_message(current_time=0.0, scheduler=scheduler)

        self.print_trace_header()
        last_time = 0.0
        for _ in range(3):
            event = scheduler.get_event()
            last_time = event.get_event_time()  # track the wall clock as we drain
            self.generate_trace(event)

        # Round 2: continue from where round 1 left off
        for client in clients:
            client.send_message(current_time=last_time, scheduler=scheduler)

        for _ in range(3):
            event = scheduler.get_event()
            self.generate_trace(event)

    # TEST GATEWAY
    def test_gateway(self):
        print("=== Testing Gateway ===")

        scheduler = Scheduler()
        queue = Queue(capacity=5)
        server = Server("1", mu_rate=2)
        gateway = Gateway(queue, [server])

        # Create one client and schedule its first message
        client = Client(1, lambda_rate=1)
        client.send_message(0.0, scheduler)

        self.print_trace_header()

        # Run until scheduler is empty or we hit a time limit
        time_limit = 10.0

        while not scheduler.is_empty():
            event = scheduler.get_event()
            current_time = event.get_event_time()

            if current_time > time_limit:
                break

            self.generate_trace(event)

            # When a SEND is processed, schedule the client's next message
            if event.get_event_type() == EventType.SEND_MSG:
                client.send_message(current_time, scheduler)

            gateway.handle_event(event, scheduler, current_time)

        print()
    # RUN ALL TESTS 
    def run_tests(self):
        self.test_message()
        self.test_event()
        self.test_scheduler()
        self.test_trace()
        self.test_queue()
        self.test_server()
        self.test_client()
        self.test_gateway()

# MAIN
if __name__ == "__main__":
    engine = Engine()

    engine.run_tests() 
