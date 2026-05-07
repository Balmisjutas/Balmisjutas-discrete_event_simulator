class Message:
    _id_counter = 1

    def __init__(self, source, destination):
        self.message_id = Message._id_counter
        Message._id_counter += 1
        self.source = source
        self.destination = destination
        self.arrival_time = None  # set when RECV happens (queue entry time / When it arrives at the gateway)
        self.send_time = None     # set when SEND happens (When client sends it)
        self.service_start_time = None # when a server actually picks it up

    # Getters
    def get_message_id(self):
        return self.message_id

    def get_source(self):
        return self.source

    def get_destination(self):
        return self.destination

    def get_arrival_time(self):
        return self.arrival_time

    def get_send_time(self):
        return self.send_time

    def get_service_start_time(self):
        return self.service_start_time

    # Setters
    def set_source(self, source):
        self.source = source

    def set_destination(self, destination):
        self.destination = destination

    def set_arrival_time(self, time):
        self.arrival_time = time

    def set_send_time(self, time):
        self.send_time = time

    def set_service_start_time(self, time):
        self.service_start_time = time

    # Print method
    def print_message(self):
        print(f"Message ID: {self.message_id}, Source: {self.source}, Destination: {self.destination}")