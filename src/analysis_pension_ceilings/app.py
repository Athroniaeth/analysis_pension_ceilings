import gradio as gr
import uvicorn
from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from analysis_pension_ceilings import logger, STATIC_PATH
from analysis_pension_ceilings.blocks import blocks

app = FastAPI()
app.mount("/static", StaticFiles(directory=STATIC_PATH), name="static")

logger.debug("Starting the Gradio application")

gradio_app = gr.mount_gradio_app(
    app=app,
    path="/",
    blocks=blocks,
)

if __name__ == "__main__":
    uvicorn.run(gradio_app, host="0.0.0.0", port=7860)
