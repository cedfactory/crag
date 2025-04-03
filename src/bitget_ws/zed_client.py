import zmq
import pandas as pd

class ZMQClient:
    def __init__(self, server_address, timeout=100):  # very short timeout
        self.server_address = server_address
        self._init_zmq_client()
        self.timeout = timeout  # timeout in milliseconds
        print(f"Client connected to {server_address}")

    def _init_zmq_client(self):
        self.context = zmq.Context()
        self._create_socket()

    def _create_socket(self):
        # Create and connect the socket, and register it with a new poller.
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.server_address)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def send_request(self, data):
        # Convert DataFrame to a dictionary if necessary.
        if isinstance(data, pd.DataFrame):
            msg = {"dataframe": data.to_dict()}
        else:
            # Serialize the data into a JSON-friendly message format
            msg = self._serialize_message(data)

        try:
            # Send the JSON message to the server
            self.socket.send_json(msg)
        except:
            return None

        # Wait for a reply with the specified timeout.
        socks = dict(self.poller.poll(self.timeout))
        if socks.get(self.socket) == zmq.POLLIN:
            try:
                reply = self.socket.recv_json()
            except:
                reply = None
            return reply
        else:
            # Reset the socket so that it's ready for the next request.
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self._create_socket()
            return {"error": "No response from server (timeout). Server might be off."}

    def _serialize_message(self, data):
        """Prepare the data (dict or DataFrame) as a JSON-serializable message dict."""
        if isinstance(data, pd.DataFrame):
            content = data.to_dict(orient="split")
            msg = {"type": "dataframe", "content": content}
        elif isinstance(data, dict):
            msg = {"type": "dict", "content": data}
        else:
            msg = {"type": "text", "content": str(data)}
        return msg

    def _deserialize_message(self, msg):
        """Convert a received message dict back into a Python object."""
        mtype = msg.get("type")
        content = msg.get("content")
        if mtype == "dataframe":
            df = pd.DataFrame(data=content["data"], columns=content["columns"])
            if "index" in content:
                df.index = content["index"]
            return df
        elif mtype == "dict":
            return content
        else:
            return content

    def close(self):
        """Close the client socket and terminate context."""
        self.socket.close()
        self.context.term()
        print("Client socket closed and context terminated.")

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the ZMQ client using pop to avoid pickling issues.
        state.pop('socket', None)
        state.pop('context', None)
        state.pop('poller', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Reinitialize the ZMQ client after unpickling.
        self._init_zmq_client()
