# BigJson

A small library to parse json schemas and output to big query schema only, for now

## Installation

    pip3 install -r requirements.txt

# Usage

## Check schema

Do not specify a GCloud project.

```
cat ../data-platform-message-schemas/dist/dereferenced/events/server/full_booking/v1.0.0/schema.json | python3 main.py
```

## Create table

Specify a GCloud project.

```
cat ../data-platform-message-schemas/dist/dereferenced/events/server/full_booking/v1.0.0/schema.json | python3 main.py hx-trial
```
