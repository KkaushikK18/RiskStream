"""HTTP routes. Handlers that touch the database or the model are plain ``def``:
FastAPI runs them in a bounded worker-thread pool, so blocking I/O never stalls
the event loop."""
