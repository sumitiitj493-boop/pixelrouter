FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download rembg's U2Net model (~170MB) at build time.
# Without this the first request would hang 30-60s while downloading.
# A 1x1 dummy image is enough to trigger the download and cache it.
# Placed before COPY . . so Docker layer caching works — code changes
# don't re-trigger the model download.
RUN python -c "\
import rembg; \
from PIL import Image; \
import io; \
img = Image.new('RGB', (1, 1), color=(255, 255, 255)); \
buf = io.BytesIO(); \
img.save(buf, format='PNG'); \
rembg.remove(buf.getvalue()); \
print('rembg model pre-downloaded successfully')"
COPY . .
EXPOSE 8002
# No --reload — this is a production image, files don't change at runtime.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]
