"""
Vercel serverless entry point for FastAPI application.
This file is required for Vercel to serve the FastAPI app as serverless functions.
"""
from mangum import Mangum
from app.main import app

# Wrap FastAPI app with Mangum for AWS Lambda/Vercel compatibility
handler = Mangum(app, lifespan="off")

