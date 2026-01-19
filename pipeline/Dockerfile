FROM python:3.13.5-slim

# copy with --from to copy from another image/stage
# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# set up our image by installing prerequisites; pandas and pyarrow in this case
# RUN pip install pandas pyarrow

# set up the working directory inside the container
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
# when i execute python, the python from the venv is used:
ENV PATH="/app/.venv/bin:$PATH"

# bc we put uv into docker, we also put related dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# copy the script to the container. 1st name is source file, 2nd is destination
COPY pipeline.py pipeline.py

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["python", "pipeline.py"]