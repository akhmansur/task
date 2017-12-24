# task

The program supports simultaneous connection of 10 clients, each client is connected to its worker through *Binds class. Each worker sends a Hello message at the beginning of his work, so that the broker remembers his id. When a new client is connected, it is bound to the first worker. If the client doesn't send messages within 10 seconds (including heartbeats), the worker sends a message to the broker asking to release it and deletes the data of that client. The client must generate his id with a size of 10 bytes and send the heartbeat at intervals of at least 5 seconds.

