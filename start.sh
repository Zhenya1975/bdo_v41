#!/bin/bash
gunicorn --bind 0.0.0.0 --preload --timeout 6000 app:app 
# gunicorn app:app