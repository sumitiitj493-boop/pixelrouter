import sys
import os
import io
import pytest
from unittest.mock import patch, MagicMock
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processor"))

def make_fake_png() -> bytes:
    img = Image.new("RGBA", (10, 10), color=(255, 0, 0, 128))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

def make_fake_jpeg() -> bytes:
    img = Image.new("RGB", (10, 10), color=(100, 150, 200))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()

def test_init_models_sets_globals():
    import pipeline
    fake_proc = MagicMock()
    fake_model = MagicMock()
    pipeline.init_models(fake_proc, fake_model)
    assert pipeline.blip_processor is fake_proc
    assert pipeline.blip_model is fake_model

def test_generate_caption_sync_raises_if_not_initialized():
    import pipeline
    pipeline.blip_processor = None
    pipeline.blip_model = None
    with pytest.raises(RuntimeError, match="not initialized"):
        pipeline.generate_caption_sync(make_fake_jpeg())

async def test_run_pipeline_returns_required_keys():
    import pipeline
    fake_png = make_fake_png()
    fake_proc = MagicMock()
    fake_model = MagicMock()
    pipeline.init_models(fake_proc, fake_model)

    with patch("pipeline.remove_background_sync", return_value=fake_png), \
         patch("pipeline.generate_caption_sync", return_value="a red square"):

        result = await pipeline.run_pipeline(
            job_id="test_job",
            image_bytes=make_fake_jpeg(),
        )

    assert "result_bytes" in result
    assert "caption" in result
    assert "status" in result
    assert "job_id" in result

async def test_run_pipeline_status_is_done():
    import pipeline
    fake_png = make_fake_png()
    pipeline.init_models(MagicMock(), MagicMock())
    with patch("pipeline.remove_background_sync", return_value=fake_png), \
         patch("pipeline.generate_caption_sync", return_value="a product"):
        result = await pipeline.run_pipeline("job_1", make_fake_jpeg())
    assert result["status"] == "done"

async def test_run_pipeline_result_bytes_is_bytes():
    import pipeline
    fake_png = make_fake_png()
    pipeline.init_models(MagicMock(), MagicMock())
    with patch("pipeline.remove_background_sync", return_value=fake_png), \
         patch("pipeline.generate_caption_sync", return_value="a product"):
        result = await pipeline.run_pipeline("job_2", make_fake_jpeg())
    assert isinstance(result["result_bytes"], bytes)
    assert len(result["result_bytes"]) > 0

async def test_run_pipeline_progress_callback_fires():
    import pipeline
    fake_png = make_fake_png()
    pipeline.init_models(MagicMock(), MagicMock())

    stages_received = []
    async def capture_callback(stage: str, progress: int):
        stages_received.append((stage, progress))

    with patch("pipeline.remove_background_sync", return_value=fake_png), \
         patch("pipeline.generate_caption_sync", return_value="test"):
        await pipeline.run_pipeline("job_3", make_fake_jpeg(), progress_callback=capture_callback)

    assert len(stages_received) >= 4
    stage_names = [s[0] for s in stages_received]
    assert "started" in stage_names
    assert "done" in stage_names

async def test_run_pipeline_callback_is_optional():
    import pipeline
    fake_png = make_fake_png()
    pipeline.init_models(MagicMock(), MagicMock())
    with patch("pipeline.remove_background_sync", return_value=fake_png), \
         patch("pipeline.generate_caption_sync", return_value="no callback"):
        result = await pipeline.run_pipeline("job_4", make_fake_jpeg(), progress_callback=None)
    assert result["status"] == "done"
