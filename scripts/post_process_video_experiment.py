#!/usr/bin/env python2
import os

print os.environ

output_file = open(os.path.join(os.environ['OUTPUT_DIR'], 'reduced_video_stats.log'), 'w')
output_file.write("test")
output_file.close()
