#!/usr/bin/env python2
import fnmatch
import os


class EventFileParser(object):

    def __init__(self, file_path):
        self.file_path = file_path

        # 0 = download started + received bytes,
        # 1 = download started, received no bytes,
        # 2 = no torrent info received,
        # 3 = no results received during remote search,
        self.status = -1

        self.tribler_startup_time = -1
        self.start_remote_query = -1
        self.first_incoming_result = -1
        self.last_incoming_result = -1
        self.received_metainfo = -1
        self.circuits_ready = -1
        self.first_byte_downloaded = -1
        self.total_bytes_downloaded = -1

    def parse_event_file(self):
        with open(self.file_path, 'r') as event_file:
            content = event_file.read()

        for line in content.split("\n"):
            if len(line) == 0:
                continue
            parts = line.split(" ")

            if parts[1] == "tribler_startup":
                self.tribler_startup_time = parts[0]
            elif parts[1] == "incoming_results":
                if self.first_incoming_result == -1:
                    self.first_incoming_result = parts[0]
                self.last_incoming_result = parts[0]
            elif parts[1] == "start_remote_search":
                self.start_remote_query = parts[0]
            elif parts[1] == "received_torrent_def" and self.received_metainfo == -1:
                self.received_metainfo = parts[0]
            elif parts[1] == "bytes_downloaded":
                if int(parts[2]) == 0:
                    self.status = 1
                else:
                    self.status = 0
                self.total_bytes_downloaded = parts[2]
            elif parts[1] == "no_torrent_info_received":
                self.status = 2
            elif parts[1] == "stopping_no_results":
                self.status = 3
            elif parts[1] == "circuits_ready":
                self.circuits_ready = parts[0]
            elif parts[1] == "download_received_first_bytes":
                self.first_byte_downloaded = parts[0]

    def write_results(self):
        with open(os.path.join(os.environ['OUTPUT_DIR'], 'reduced_video_stats.log'), 'a') as output_file:
            output_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (self.status, self.tribler_startup_time, self.start_remote_query, self.first_incoming_result, self.last_incoming_result, self.received_metainfo, self.circuits_ready, self.first_byte_downloaded, self.total_bytes_downloaded))

#EventFileParser("test_event.log")

for root, dirnames, filenames in os.walk(os.environ['OUTPUT_DIR']):
    for filename in fnmatch.filter(filenames, 'events_*'):
        parser = EventFileParser(os.path.join(root, filename))
        parser.parse_event_file()
        parser.write_results()
