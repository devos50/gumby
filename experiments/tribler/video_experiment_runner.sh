#!/bin/bash

export LD_LIBRARY_PATH=$VIRTUALENV_DIR/lib:$LD_LIBRARY_PATH

python gumby/experiments/tribler/video_experiment_runner.py
