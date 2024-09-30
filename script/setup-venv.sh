#!/bin/bash

current_dir=$(pwd)
last_folder=$(basename "$current_dir")

if [ "$last_folder" == "script" ]; then
    cd ..
fi

echo "Create python3 venv"
python3 -m venv venv

echo "Activate the virtual environment"
source venv/bin/activate

echo "Install library"
pip install -U pip
pip install -r requirements.txt
