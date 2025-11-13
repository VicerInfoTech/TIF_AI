# Entrypoint to run the FastAPI app with Uvicorn
from app.main import app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
