#!/usr/bin/env python3
"""
Simple HTTP server for Celery worker to satisfy Render's web service requirements.
This allows the worker to run as a web service instead of a paid background worker.
"""
import os
import signal
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread


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


def run_http_server(port):
    """Run a simple HTTP server on the specified port."""
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"HTTP server started on port {port}")
    server.serve_forever()


def run_celery_worker():
    """Run the Celery worker."""
    print("Starting Celery worker...")
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 'app.worker.celery_app',
        'worker',
        '--loglevel=info'
    ]
    process = subprocess.Popen(cmd)
    return process


def main():
    """Main entry point."""
    port = int(os.environ.get('PORT', 10000))
    
    # Start Celery worker
    celery_process = run_celery_worker()
    
    # Handle shutdown signals
    def signal_handler(sig, frame):
        print("\nShutting down...")
        celery_process.terminate()
        celery_process.wait()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start HTTP server in main thread
    try:
        run_http_server(port)
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == '__main__':
    main()

