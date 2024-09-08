#!/bin/bash
pytest tests/
find . -type d -name "__pycache__" -exec rm -rf {} +
rm -rf .pytest_cache/
rm -rf ./starstream/trivials
rm -rf data/

