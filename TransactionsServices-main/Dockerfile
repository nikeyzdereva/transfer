FROM python:3.10-slim

RUN mkdir /gui
WORKDIR /gui

COPY . .
RUN apt-get update && apt-get install -y \
    python3-tk \
    tk-dev
RUN pip install -r requirements.txt
ENV DISPLAY=:0.0
CMD [ "python3", "view.py"]