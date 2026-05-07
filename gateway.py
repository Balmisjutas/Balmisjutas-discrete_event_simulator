from event import Event, EventType


class Gateway:
    def __init__(self, queue, servers):
        self.queue = queue
        self.servers = servers  # list of servers

    # Find an idle server
    def get_idle_server(self):
        for server in self.servers:
            if not server.get_is_busy():
                return server
        return None

    # HANDLE EVENTS 
    def handle_event(self, event, scheduler, current_time):
        event_type = event.get_event_type()
        message = event.get_message()

        # SEND - schedule RECV
        if event_type == EventType.SEND_MSG:
            recv_time = current_time + 1  # fixed delay
            new_event = Event(recv_time, EventType.RECV_MSG, message)
            scheduler.add_event(new_event)

        # RECV - process or queue
        elif event_type == EventType.RECV_MSG:
            server = self.get_idle_server()

            if server:
                service_time = server.start_service(message)
                dept_time = current_time + service_time

                # Update message source - now server owns it
                message.set_source(server.get_server_id())

                new_event = Event(dept_time, EventType.MSG_DEPT, message)
                scheduler.add_event(new_event)

            else:
                accepted = self.queue.enqueue(message)
                if not accepted:
                    print(f"  *** Message {message.get_message_id()} DROPPED - queue full! ***")

        # DEPT - finish and check queue
        elif event_type == EventType.MSG_DEPT:
            server_id = message.get_source()

            # Find the server that was processing
            server = next(s for s in self.servers if s.get_server_id() == server_id)

            server.finish_service()

            if not self.queue.is_empty():
                next_msg = self.queue.dequeue()
                service_time = server.start_service(next_msg)

                next_msg.set_source(server.get_server_id())

                dept_time = current_time + service_time
                new_event = Event(dept_time, EventType.MSG_DEPT, next_msg)
                scheduler.add_event(new_event)