FROM python:3.12-slim

WORKDIR /script

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

CMD [ "python", "main.py" ]