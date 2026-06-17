FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install CPU-only PyTorch FIRST — much smaller (~150MB vs 2.5GB GPU version)
RUN pip install --no-cache-dir --default-timeout=1000 \
    torch==2.3.0 torchvision \
    --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

# Pre-download rembg's U2Net model (~170MB) at build time.
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
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002"]