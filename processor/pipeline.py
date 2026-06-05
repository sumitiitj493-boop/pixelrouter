# PixelRouter — AI Processing Pipeline
# rembg (background removal) + BLIP (image captioning)

import asyncio
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable

import rembg
from PIL import Image
from transformers import BlipProcessor, BlipForConditionalGeneration

logger = logging.getLogger("pipeline")

# rembg and BLIP are blocking — they hog the event loop if we don't
# offload them. 2 workers is enough since BLIP already saturates CPU cores
# internally; spawning more would just cause contention.
executor = ThreadPoolExecutor(max_workers=2)

# Loaded once at startup via init_models(), not at import time —
# these models are ~900MB and loading them during import would kill
# the container health check.
blip_processor: Optional[BlipProcessor] = None
blip_model: Optional[BlipForConditionalGeneration] = None


def init_models(processor: BlipProcessor,
                model: BlipForConditionalGeneration) -> None:
    """Wiring point — app.py calls this during lifespan startup."""
    global blip_processor, blip_model
    blip_processor = processor
    blip_model = model
    logger.info("Pipeline models initialized successfully")


# ── Stage 1: Background Removal ──────────────────────────────────────────────

def remove_background_sync(image_bytes: bytes) -> bytes:
    """Sync worker for rembg — called inside run_in_executor."""
    logger.info("Starting background removal with rembg")
    result = rembg.remove(image_bytes)
    logger.info(f"Background removal complete, output size: {len(result)} bytes")
    return result


async def remove_background(image_bytes: bytes) -> bytes:
    """Thin async wrapper so callers can await without blocking the loop."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor,
        remove_background_sync,
        image_bytes
    )
    return result


# ── Stage 2: Image Captioning ─────────────────────────────────────────────────

def generate_caption_sync(image_bytes: bytes) -> str:
    """Sync worker for BLIP inference — called inside run_in_executor."""
    if blip_processor is None or blip_model is None:
        raise RuntimeError(
            "BLIP models not initialized. Call init_models() first."
        )

    logger.info("Starting BLIP caption generation")

    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")

    # Correct modern call (this was the main fix)
    inputs = blip_processor(images=image, return_tensors="pt")

    output = blip_model.generate(**inputs, max_new_tokens=50)
    caption = blip_processor.decode(output[0], skip_special_tokens=True)

    logger.info(f"Caption generated: {caption}")
    return caption


async def generate_caption(image_bytes: bytes) -> str:
    """Async wrapper — same executor pattern as remove_background."""
    loop = asyncio.get_event_loop()
    caption = await loop.run_in_executor(
        executor,
        generate_caption_sync,
        image_bytes
    )
    return caption


# ── Main Pipeline ─────────────────────────────────────────────────────────────

async def run_pipeline(
    job_id: str,
    image_bytes: bytes,
    progress_callback: Optional[Callable] = None
) -> dict:
    """
    Full pipeline: background removal → caption → return result.

    progress_callback is Optional so unit tests can call this without
    wiring up a WebSocket — if it's None we just skip the emit.
    """
    logger.info(f"[{job_id}] Pipeline starting")

    async def emit(stage: str, progress: int):
        """Tiny helper — no-op when there's no callback (e.g. in tests)."""
        if progress_callback is not None:
            await progress_callback(stage, progress)
        logger.info(f"[{job_id}] Stage: {stage} ({progress}%)")

    await emit("started", 5)

    await emit("removing_background", 20)
    result_bytes = await remove_background(image_bytes)

    await emit("background_removed", 60)
    logger.info(f"[{job_id}] Background removed, result: {len(result_bytes)} bytes")

    # Feed the *clean* image (after bg removal) to BLIP so the caption
    # describes just the product, not whatever was behind it.
    await emit("generating_caption", 65)
    caption = await generate_caption(result_bytes)

    await emit("caption_generated", 90)
    logger.info(f"[{job_id}] Caption: {caption}")

    await emit("finalizing", 95)

    result = {
        "job_id": job_id,
        "result_bytes": result_bytes,
        "caption": caption,
        "status": "done"
    }

    await emit("done", 100)
    logger.info(f"[{job_id}] Pipeline complete")

    return result