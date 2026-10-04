FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY discord_radio_bot.py radio_phrases.example.txt ./

RUN useradd --create-home --uid 10001 bot && mkdir /usr/src/app/data && chown bot:bot /usr/src/app/data
USER bot

CMD ["python", "discord_radio_bot.py"]
