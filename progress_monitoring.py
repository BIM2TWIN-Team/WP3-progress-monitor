# -*- coding: utf-8 -*-`
# Copyright University of Cambridge 2023. All Rights Reserved.
# Author: Alwyn Mathew <am3156@cam.ac.uk>
# This file cannot be used without a written permission from the author(s).

import argparse
from datetime import datetime

from tqdm import tqdm

from DTP_API.DTP_API import DTPApi
from DTP_API.DTP_config import DTPConfig


def activity_status(time_list):
    """
    Get combined status from a list of task status

    Parameters
    ----------
    time_list: list, obligatory

    Returns
    -------
    str
        Final status from a list of tasks
    """
    status = ['ahead', 'behind', 'on']
    counts = [time_list.count(s) for s in status]
    return status[counts.index(max(counts))]


def get_num_days(each_activity_tracker, computed_status):
    """
    Get number of days an activity is behind/ahead

    Parameters
    ----------
    each_activity_tracker: dict, obligatory
        Dictionary that stores the status and number of days each task is ahead/behind
    computed_status: str, obligatory
        Status of the activity
    Returns
    -------
    int
        Number of days an activity is behind/ahead
    """
    num_days_list = []
    for idx, status in enumerate(each_activity_tracker['status']):
        if status == computed_status:
            num_days_list.append(each_activity_tracker['days'][idx])
    return max(num_days_list)


def check_schedule(activity_start_time, activity_end_time, operation_start_time, operation_end_time,
                   activity_progress):
    """
    Deside if the activity is ahead/behind/on schedule

    Parameters
    ----------
    activity_start_time: datetime.datetime, obligatory
        Start time for activity
    activity_end_time: datetime.datetime, obligatory
        End time for activity
    operation_start_time: datetime.datetime, obligatory
        Start time for operation
    operation_end_time: datetime.datetime, obligatory
        End time for operation
    activity_progress: str, obligatory
        Progress of an element in operation

    Returns
    -------
    tuple
        Progress status [ahead, behind, on], number of days ahead/behind, completed-1 or 0-not complete
    """
    if activity_progress == '100':
        if activity_end_time > operation_end_time:
            combined_status = 'ahead', (activity_end_time - operation_end_time).days, 1
        elif activity_end_time < operation_end_time:
            combined_status = 'behind', (operation_end_time - activity_end_time).days, 1
        else:
            combined_status = 'on', 0, 1
    else:
        if activity_start_time > operation_start_time:
            combined_status = 'on', -1, 0
        elif activity_start_time < operation_start_time:
            combined_status = 'behind', (operation_end_time - activity_end_time).days, 0
        else:
            combined_status = 'on', -1, 0

    return combined_status


class ProgressMonitor:

    def __init__(self, dtp_config, dtp_api):
        """
        Parameters
        ----------
        dtp_config : DTP_Config, obligatory
            an instance of DTP_Config
        dtp_api : DTP_Api, obligatory
            an instance of DTP_Api
        """
        self.DTP_CONFIG = dtp_config
        self.DTP_API = dtp_api

    def __get_progress_from_as_performed_node(self, node):
        """
        Get progress of each as-performed node

        Parameters
        ----------
        node: dict,
            JSON mapped to a dictionary. The data contain nodes of the type element.
        Returns
        -------
        str
            The progress of the node
        """
        return node['items'][0][self.DTP_CONFIG.get_ontology_uri('progress')]

    def __get_time(self, node, as_planned):
        """
        Get start and end time of a node

        Parameters
        ----------
        node: dict
            directory with node information
        as_planned: bool
            if true get time for as-planned node else as-performed
        Returns
        -------
        tuple
            Start and end date of the node
        """
        uri_str = 'planned' if as_planned else 'process'
        start_time = node[self.DTP_CONFIG.get_ontology_uri(uri_str + 'Start')]
        end_time = node[self.DTP_CONFIG.get_ontology_uri(uri_str + 'End')]
        return datetime.fromisoformat(start_time), datetime.fromisoformat(end_time)

    def __get_as_performed_op_node(self, as_planned_node):
        """
        Get as-performed operation node from as-planned node

        Parameters
        ----------
        as_planned_node: dict
            Dictionary with node information
        Returns
        -------
        dict
            JSON mapped to a dictionary of an as-performed node.
        """
        return self.DTP_API.fetch_asperformed_connected_asdesigned_oper_nodes(as_planned_node['_iri'])

    def __get_as_performed_element(self, as_planned_node):
        """
        Get as-performed element node from as-planned node

        Parameters
        ----------
        as_planned_node: dict
            Dictionary with node information
        Returns
        -------
        dict
            JSON mapped to a dictionary of an as-performed node.
        """
        return self.DTP_API.fetch_asperformed_connected_asdesigned_nodes(as_planned_node['items'][0]['_iri'])

    def compute_progress_at_activity(self, activities=None):
        """
        Compute progress at activity level

        Parameters
        ----------
        activities: dict
            Query response of all activity node
        Returns
        -------
        dict
            Dictionary contains the percentage of tasks finished, ahead/behind/on schedule and how many days for each
            activity
        """
        if activities is None:
            print("Started querying all activity nodes from DTP...")
            activities = self.DTP_API.query_all_pages(self.DTP_API.fetch_activity_nodes)
            print("Completed fetching all activity nodes from DTP.")
        activity_tracker = dict()
        progress_at_activity = dict()

        print("Started progress monitering...")
        for each_activity in tqdm(activities['items']):
            activity_tracker[each_activity['_iri']] = {'complete': [], 'status': [], 'days': []}
            operation = self.__get_as_performed_op_node(each_activity)['items'][0]

            activity_start_time, activity_end_time = self.__get_time(each_activity, as_planned=True)
            operation_start_time, operation_end_time = self.__get_time(operation, as_planned=False)

            if not operation['size']:  # if as-planned node doesn't have an as-performed node
                activity_tracker[each_activity['_iri']]['complete'].append(0)
                activity_tracker[each_activity['_iri']]['days'].append(-1)  # -1 for not started
                if activity_start_time < operation_start_time:
                    # if operation needed to be started but not started yet
                    activity_tracker[each_activity['_iri']]['status'].append('behind')
                else:
                    activity_tracker[each_activity['_iri']]['status'].append('on')
                continue

            tasks = self.DTP_API.query_all_pages(self.DTP_API.fetch_activity_connected_task_nodes,
                                                 each_activity['_iri'])
            for each_task in tasks['items']:
                as_planned_element = self.DTP_API.fetch_elements_connected_task_nodes(each_task['_iri'])
                as_performed_element = self.__get_as_performed_element(as_planned_element)
                if not as_performed_element['size']:  # if as-planned node doesn't have an as-performed node
                    continue

                as_performed_status = self.__get_progress_from_as_performed_node(as_performed_element)
                time_status, days, task_complete_flag = check_schedule(activity_start_time, activity_end_time,
                                                                       operation_start_time, operation_end_time,
                                                                       as_performed_status)
                activity_tracker[each_activity['_iri']]['complete'].append(task_complete_flag)
                activity_tracker[each_activity['_iri']]['days'].append(days)
                activity_tracker[each_activity['_iri']]['status'].append(time_status)

            num_complete_task = len(activity_tracker[each_activity['_iri']]['complete'])
            if num_complete_task:
                computed_complete = sum(activity_tracker[each_activity['_iri']]['complete']) / num_complete_task * 100
                computed_status = activity_status(activity_tracker[each_activity['_iri']]['status'])
                computed_num_days = get_num_days(activity_tracker[each_activity['_iri']], computed_status)
                progress_at_activity[each_activity['_iri']] = {'complete': computed_complete,
                                                               'status': computed_status,
                                                               'days': computed_num_days}
            else:  # if activity node with no corresponding action nodes
                progress_at_activity[each_activity['_iri']] = {'complete': 0.0, 'status': 'behind', 'days': -1}

        return progress_at_activity


def parse_args():
    """
    Get parameters from user
    """
    parser = argparse.ArgumentParser(description='Perform progress monitoring with DTP graph')
    parser.add_argument('--xml_path', '-x', type=str, help='path to config xml file', default='DTP_API/DTP_config.xml')
    parser.add_argument('--simulation', '-s', default=False, action='store_true')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dtp_config = DTPConfig(args.xml_path)
    dtp_api = DTPApi(dtp_config, simulation_mode=args.simulation)
    progress_monitor = ProgressMonitor(dtp_config, dtp_api)
    progress_dict = progress_monitor.compute_progress_at_activity()
    for activity_iri, progress in progress_dict.items():
        print(activity_iri, progress)
