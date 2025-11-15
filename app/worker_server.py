#!/usr/bin/env python3
"""
Simple HTTP server for Celery worker to satisfy Render's web service requirements.
This allows the worker to run as a web service instead of a paid background worker.
"""
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple health check handler."""
    
    def do_GET(self):
        """Handle GET requests - return 200 OK for health checks."""
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Celery worker is running')
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def run_celery_worker():
    """Run the Celery worker in a separate process."""
    print("Starting Celery worker...", flush=True)
    # Get concurrency from environment or use default
    concurrency = os.environ.get('CELERY_CONCURRENCY', '2')
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 'app.worker.celery_app',
        'worker',
        '--loglevel=info',
        f'--concurrency={concurrency}'
    ]
    process = subprocess.Popen(cmd)
    return process


def main():
    """Main entry point."""
    # Get port from environment variable (required by Render)
    # Render dynamically assigns a port via the PORT environment variable
    port = int(os.environ.get('PORT', 10000))
    print(f"PORT environment variable: {os.environ.get('PORT', 'not set (using default 10000)')}", flush=True)
    print(f"Starting HTTP server on 0.0.0.0:{port}", flush=True)
    
    # Store celery process for cleanup
    celery_process_container = {'process': None}
    
    def start_celery():
        """Start Celery worker after a short delay."""
        time.sleep(2)  # Give HTTP server time to start first
        celery_process_container['process'] = run_celery_worker()
    
    celery_thread = threading.Thread(target=start_celery, daemon=True)
    celery_thread.start()
    
    # Handle shutdown signals
    def signal_handler(sig, frame):
        print("\nShutting down...", flush=True)
        if celery_process_container['process']:
            celery_process_container['process'].terminate()
            celery_process_container['process'].wait()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start HTTP server immediately (this is what Render checks for)
    # Must bind to 0.0.0.0 (all interfaces) not 127.0.0.1 (localhost only)
    
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        print(f"HTTP server successfully bound to 0.0.0.0:{port}", flush=True)
        print(f"Server is ready and listening for connections", flush=True)
        sys.stdout.flush()
        server.serve_forever()
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        print(f"Error starting HTTP server: {e}", flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

