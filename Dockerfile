FROM python:3.12.7-slim

RUN pip install uv

WORKDIR /app
COPY README.md pyproject.toml requirements.lock ./
RUN uv pip install --no-cache --system -r requirements.lock

COPY src ./src
RUN python src/analysis_pension_ceilings download
CMD python src/analysis_pension_ceilings run
