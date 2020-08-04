import logging
from threading import Lock
from automation import CommandSequence, TaskManager, BrowserManager
from argparse import RawTextHelpFormatter
import argparse
import os
import sqlite3 as lite
import json
import time
from queue import Empty as EmptyQueue

from typing import Any, Callable, List

########################################################################
#				Global Variables and Constants 						   #
########################################################################
# TODO Move them to a file?
NUM_BROWSERS = 1 if os.cpu_count() < 4 else 10 # Automatically adjust amount of browsers if we run on a more powerfull machine.
DEFAULT_PROFILE_TYPE = 0
DEFAULT_INPUT_FILE =  "/home/openwpm/Desktop/final_links_to_crawl.csv"
LINKS_TO_COLLECT = 5 if os.cpu_count() < 4 else 150 # Automatically adjust if we profit of a more powerfull machine.
SITES_TO_CRAWL = 10000
COLLECT_LINKS = False
BASE_DATA_DIR = '~/Desktop/'
PART = 0
BASE_OUTPUT_SQLITE_NAME = "full_crawl-data.sqlite"
DEBUG = False

########################################################################
#							Functions 								   #
########################################################################


def run_crawl(sites, profile_type=DEFAULT_PROFILE_TYPE):
    # Loads the default manager params
    # and NUM_BROWSERS copies of the default browser params
    manager_params, browser_params = TaskManager.load_default_params(NUM_BROWSERS)

    # Update browser configuration (use this for per-browser settings)
    for i in range(NUM_BROWSERS):
        # ~~~
        # ~~~ General Stats for All Browsers
        # ~~~
        # Record HTTP Requests and Responses
        browser_params[i]['http_instrument'] = True
        # Record cookie changes
        browser_params[i]['cookie_instrument'] = True
        # Record Navigations
        browser_params[i]['navigation_instrument'] = True
        # Record JS Web API calls
        browser_params[i]['js_instrument'] = True
        # Record the callstack of all WebRequests made
        browser_params[i]['callstack_instrument'] = True
        # DNS instrumentation
        browser_params[i]['dns_instrument'] = True
        # Launch only browser in xvfb mode
        browser_params[i]['display_mode'] = 'native'
        # Use bot mitigation techniques
        browser_params[i]['bot_mitigation'] = True
        # Define the profile type
        browser_params[i]['profile_type'] = profile_type
        # Use an existing profile
        browser_params[i]['profile_tar'] = "/home/openwpm/Desktop"

        
        # ~~~
        # ~~~ Profile specific
        # ~~~
        if profile_type == 0:
            # No spezial config
            # browser_params[i]['js_instrument_settings'] = {"window": ["postMessage",]}
            pass
        elif profile_type == 1:
            # Deny all 3rd-party cookies
            browser_params[i]['tp_cookies'] = "never"
        elif profile_type == 2:
            # Enable uBlock origin
            browser_params[i]['ublock-origin'] = True
        elif profile_type == 3:
            # Enable uBlock origin and deny all 3rd-party cookies
            browser_params[i]['ublock-origin'] = True
            browser_params[i]['tp_cookies'] = "never"

    # Update TaskManager configuration (use this for crawl-wide settings)
    manager_params['data_directory'] = BASE_DATA_DIR
    manager_params['log_directory'] = BASE_DATA_DIR
    manager_params['database_name'] = "full_crawl-data.sqlite"
    manager_params['log_file'] = "full_crawl_openwpm.log"

    # Instantiates the measurement platform
    manager = TaskManager.TaskManager(manager_params, browser_params)

    # Visits the sites
    for site in sites:
        site_token = site.split(',')
        site_to_visit = site_token[1]
        site_rank = int(site_token[0])
        
        # Parallelize sites over all number of browsers set above.
        command_sequence = CommandSequence.CommandSequence(
            site_to_visit, reset=True,
            site_rank=site_rank,
            callback=lambda val=site: print("CommandSequence {} done".format(val)))
        print("get", site)
        command_sequence.get(sleep=40, timeout=75)
        
        # Run commands across the three browsers (simple parallelization)
        manager.execute_command_sequence(command_sequence)

    # Shuts down the browsers and waits for the data to finish logging
    manager.close()


def get_all_sites_that_should_have_been_visited():
    """
    Returns all Sites, that should have been visited.
    """
    input_sites = set()
    sites_and_rank = dict()
    with open(DEFAULT_INPUT_FILE, "r") as temp_file:
        sites = [line.strip() for line in temp_file]

    for site in sites:
        if site == '':
            continue
        site_token = site.split(',')
        input_sites.add(site_token[1])
        sites_and_rank[site_token[1]] = site_token[0]
        

    # input_sites = select_sites_to_crawl(list(input_sites), part, top_sites)

    return input_sites, sites_and_rank


def get_all_visited_sites():
    """"
    Prints all relevant informations of the rows of crawl_history Datatable to the screen.
    """
    visited_sites = set()
    conn = lite.connect("/home/openwpm/Desktop/" + BASE_OUTPUT_SQLITE_NAME)
    cur = conn.cursor()
    for crawl_id, visit_id, command, arguments, retry_number, command_status, error, traceback, dtg in conn.execute(
            "SELECT * FROM crawl_history"):
        args = json.loads(arguments)
        if "url" in args:
            visited_sites.add(args["url"]) #.replace("https://www.", "")
    #print("visited_sites",visited_sites)
    return visited_sites


def print_urls(urls):
    """
    Prints all Urls in url.
    """
    counter = 1
    for url in urls:
        print("\t{nr}: {url}".format(nr=counter, url=url))
        counter += 1


def get_all_sites_that_were_not_crawled():
    """
    Returns all sites, that was not crawled in the first crawl.
    """
    #print("#################################################")
    print("INITIATE NOT CRAWLED SITES RECRAWL")
    input_sites, sites_and_rank = get_all_sites_that_should_have_been_visited()
    visited_sites = get_all_visited_sites()

    not_crawled = input_sites - visited_sites

    if DEBUG:
        print("!!!ALL uncrawled Sites!!!\n")
        print_urls(not_crawled)
        print("\n")
        #print("\n")
    
    to_crawl = list()
    for site in not_crawled:
        to_crawl.append(str(sites_and_rank[site])+ "," + str(site))

    return to_crawl


def initiate_crawl_missed(profile):
    """
    Executes an recrawl of all uncrawled urls of the main crawl.
    """
    not_crawled = get_all_sites_that_were_not_crawled()
    # print("#############################################################\n")
    print("Overall, %i sites were missed in the previous crawl(s)." % (len(not_crawled)))
    # print("#############################################################\n")
    print("\n")
    # print("\n")
    time.sleep(3)

    run_crawl(not_crawled, profile)

    print("\n")
    print("###################### FINAL SAINITY CHECK ############################")
    input_sites = get_all_sites_that_should_have_been_visited()[0]
    visited_sites = get_all_visited_sites()
    not_crawled = input_sites - visited_sites
    print("visited_sites", len(visited_sites))
    print("not_crawled", len(not_crawled))
    print("input_sites", len(input_sites))
    #print(visited_sites)
    se = visited_sites - input_sites
    print("should be empty", len(se))

    #print(se)
########################################################################
#						 Main 										   #
########################################################################
def main():
    # Define and parse command line arguments
    parser = argparse.ArgumentParser(description='Crawls a set of given websites unsing a defined profile', formatter_class=RawTextHelpFormatter)
    parser.add_argument('--profile', dest='profile', default=DEFAULT_PROFILE_TYPE, type=int, choices=range(0, 4),
                        help='Defines the profile to use: \n\t 0 = no special option\n\t 1 = no third-party cookies \n\t 2 = enable uBlock-origin\n\t 3 = no third-party cookies and use uBlock-origin \n(Default: 0)')
    parser.add_argument('--inputFile', dest='input_file', default=DEFAULT_INPUT_FILE,
                        help='Defines the input file (Default: sites_to_crawl.csv).')
    parser.add_argument('--crawlMissed', dest='crawlMissed', default=False, action='store_const', const=True, help='Detects all uncrawled links and recrawl that links.')
    args = parser.parse_args()
    crawlMissed = args.crawlMissed
    print("I am using the follwoing command line arguments:", args)

    if crawlMissed:
        initiate_crawl_missed(args.profile)
    else:
        # Read sites to crawl
        with open(args.input_file, "r") as temp_file:
            sites = [line.strip() for line in temp_file]
    sites=['1337,file:///home/openwpm/Desktop/test1/Page1.html']
    run_crawl(sites, profile_type=args.profile)


if __name__ == "__main__":
    # execute only if run as a script
    main()
