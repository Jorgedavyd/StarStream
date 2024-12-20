#!/bin/bash
python3 -m pip install --upgrade pip
pip install pytest pipreqs pip-tools setuptools wheel
pipreqs starstream --savepath=./requirements.in
sed -i 's/==.*$//' requirements.in
sort requirements.in | uniq > requirements_unique.in
pip-compile ./requirements_unique.in
rm -f *.in
pip install -r requirements_unique.txt --ignore-installed
rm -f requirements_unique.txt
playwright install
