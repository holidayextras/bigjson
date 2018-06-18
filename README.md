# BigJson

A small library to parse json schemas and output to big query schema only, for now

# Usage

```
$ pip3 install google-cloud-bigquery
$ cat ../data-platform-message-schemas/dist/dereferenced/events/server/full_booking/v1.0.0/schema.json | python3 main.py hx-trial collector__streaming
```
