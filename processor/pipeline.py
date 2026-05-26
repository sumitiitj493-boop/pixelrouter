# PixelRouter — AI Processing Pipeline
# Responsibility: Run background removal (rembg) and
#                 image captioning (BLIP-base) on uploaded images.
#
# Pipeline stages:
#   Stage 1 — Load image from GCS
#   Stage 2 — Remove background using rembg (U2Net model)
#   Stage 3 — Generate caption using BLIP-base (transformers)
#   Stage 4 — Save processed image to GCS
#   Stage 5 — Return result URL + caption
#
# Memory notes:
#   - Load BLIP model ONCE at startup, not per request
#   - Use BLIP-base (not large) — fits in 16GB RAM with 2 instances
#   - rembg downloads U2Net model on first run — pre-download in Dockerfile
#
# TODO : Implement remove_background()
# TODO : Implement generate_caption()
# TODO : Implement run_pipeline()

async def remove_background(image_bytes: bytes) -> bytes:
    """
    Remove background from image using rembg.
    Returns PNG bytes with transparent background.
    TODO: Implement on Upcoming Days
    """
    raise NotImplementedError("Implement on Upcoming Days")


async def generate_caption(image_bytes: bytes) -> str:
    """
    Generate a text caption for the image using BLIP-base.
    Returns caption string.
    TODO: Implement on Upcoming Days
    """
    raise NotImplementedError("Implement on Upcoming Days")


async def run_pipeline(job_id: str, image_bytes: bytes,
                       progress_callback=None) -> dict:
    """
    Full pipeline: remove background → generate caption → save to GCS.
    Calls progress_callback at each stage for WebSocket streaming.
    TODO: Implement on Upcoming Days
    """
    raise NotImplementedError("Implement on Upcoming Days")
