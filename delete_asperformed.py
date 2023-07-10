# -*- coding: utf-8 -*-`

# Copyright University of Cambridge 2023. All Rights Reserved.
# Author: Alwyn Mathew <am3156@cam.ac.uk>
# This file cannot be used without a written permission from the author(s).

import argparse

from tqdm import tqdm

from DTP_API.DTP_API import DTPApi
from DTP_API.DTP_config import DTPConfig


class DeleteAsPerformed:
    """
    The class deletes as performed nodes except element level

    Attributes
    ----------
    DTP_CONFIG : class
        an instance of DTP_Config
    DTP_API : DTP_Api, obligatory
            an instance of DTP_Api

    Methods
    -------
    delete_asperf_nodes(node_level)
        dict, number of action, operation, and construction nodes deleted
    """

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
        self.deleted_nodes_num = {'action': 0, 'operation': 0, 'construction': 0}

    def delete_asperf_nodes(self, node_level):
        """
        Delete as-performed nodes
        """
        if node_level == "construction":
            fetch_fn = self.DTP_API.fetch_construction_nodes
        elif node_level == "operation":
            fetch_fn = self.DTP_API.fetch_op_nodes
        elif node_level == "action":
            fetch_fn = self.DTP_API.fetch_action_nodes
        else:
            raise Exception("Wrong node level!")
        print(f"Started querying {node_level} nodes ")
        all_nodes = self.DTP_API.query_all_pages(fetch_fn)
        print(f"Deleting {node_level} nodes")
        for each_node in tqdm(all_nodes['items']):
            self.DTP_API.delete_node_from_graph_with_iri(each_node['_iri'])
            self.deleted_nodes_num[node_level] += 1
        print(f"Finished deleting {node_level} nodes.")


def parse_args():
    """
    Get parameters from user
    """
    parser = argparse.ArgumentParser(description='Delete as-performed nodes in DTP graph')
    parser.add_argument('--xml_path', '-x', type=str, help='path to config xml file', default='DTP_API/DTP_config.xml')
    parser.add_argument('--simulation', '-s', default=False, action='store_true')
    parser.add_argument('--target_level', '-t', type=str, choices=['construction', 'operation', 'action', 'all'],
                        help='node level to be deleted', required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dtp_config = DTPConfig(args.xml_path)
    dtp_api = DTPApi(dtp_config, simulation_mode=args.simulation)
    delete_as_performed = DeleteAsPerformed(dtp_config, dtp_api)
    if args.target_level in ['construction', 'all']:
        delete_as_performed.delete_asperf_nodes('construction')
    elif args.target_level in ['operation', 'all']:
        delete_as_performed.delete_asperf_nodes('operation')
    elif args.target_level in ['action', 'all']:
        delete_as_performed.delete_asperf_nodes('action')

    print(f"Deleted "
          f"{delete_as_performed.deleted_nodes_num['construction']} construction, "
          f"{delete_as_performed.deleted_nodes_num['operation']} operation and "
          f"{delete_as_performed.deleted_nodes_num['action']} action nodes.")
