#!/bin/bash
environment_name='lemon-step-size-remainder'
python3 -m venv $environment_name
source $environment_name/bin/activate
pip3 install --upgrade pip
pip3 install pip-upgrader
pip-upgrade
pip3 install -r requirements.txt
