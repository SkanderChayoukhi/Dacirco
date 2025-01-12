#! /usr/bin/python3
# coding: utf-8

import argparse
import csv
import logging
import random

import numpy.random


class Scenario:
    def __init__(self, nb_instances, filename):
        self.output_file = filename
        self.nb_instances = nb_instances

        self.list_times = self.createTimes(6, 2)
        self.list_movies = self.createMovies()

        self.entries = []
        for i in range(self.nb_instances):
            new_row = list()
            new_row.append(self.list_times[i])
            new_row.append(self.list_movies[i])
            new_row.append(random.randint(500, 8000))
            new_row.append(random.choice(["ultrafast", "fast"]))
            self.entries.append(new_row)

    def createTimes(self, mean_interval, sigma_interval):
        """create nb_instances of times based on Gauss

        Returns:
           list of floats"""

        times = list()
        times.append(0)
        # last = 0
        for _ in range(self.nb_instances):
            next_time = random.gauss(mean_interval, sigma_interval)
            t = max(0.1, next_time)  # + last
            times.append(t)
            # last = t
        return times

    def createMovies(self):
        return [f"vid-{random.randint(0, 40):02}.mp4" for _ in range(self.nb_instances)]

    def saveInFile(self):
        with open(self.output_file, "w", newline="") as out_file:
            f = csv.writer(out_file, delimiter=",", quotechar='"')
            for new_row in self.entries:
                f.writerow(new_row)


def main():
    ### Command line arguments parsing
    parser = argparse.ArgumentParser(description="The generator of scenario")
    parser.add_argument(
        "-d",
        "--debug",
        dest="debugFlag",
        help="Raise the log level to debug",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-f", "--file", dest="outputFile", help="the name of the scenario file"
    )
    parser.add_argument(
        "-n",
        "--nb",
        dest="nb_instances",
        type=int,
        default=5,
        help="the number of rows in the csv",
    )
    args = parser.parse_args()

    ### Log level configuration
    if args.debugFlag is True:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.WARNING
    logging.basicConfig(level=logLevel)

    scenario = Scenario(args.nb_instances, args.outputFile)
    scenario.saveInFile()


### Start Application if directly called from command line
if __name__ == "__main__":
    main()
