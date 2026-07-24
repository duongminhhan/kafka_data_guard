FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY remediation ./remediation
COPY scripts/test_transaction.py ./scripts/test_transaction.py

RUN useradd --create-home --uid 10001 remediation \
    && chown -R remediation:remediation /app

USER remediation

EXPOSE 8090

CMD ["python", "-m", "uvicorn", "remediation.api.main:app", "--host", "0.0.0.0", "--port", "8090"]
