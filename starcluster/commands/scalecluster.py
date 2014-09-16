# Copyright 2009-2014 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

from starcluster import exception
from starcluster.balancers import alt

from completers import ClusterCompleter


class CmdScaleCluster(ClusterCompleter):
    """
    scalecluster <cluster_tag>

    Start the SGE Load Balancer.

    """

    names = ['scalecluster']


    def addopts(self, parser):
        parser.add_option("--spot_bid", dest="spot_bid",
                          action="store", type="float", default=None,
                          help="spot bid price")
        parser.add_option("--max_to_add", dest="max_to_add",
                          action="store", type="int", default=None,
                          help="max_to_add")
        parser.add_option("--time_to_add_servers_fixed", dest="time_to_add_servers_fixed",
                          action="store", type="int", default=None,
                          help="time_to_add_servers_fixed")
        parser.add_option("--time_to_add_servers_per_server", dest="time_to_add_servers_per_server",
                          action="store", type="int", default=None,
                          help="time_to_add_servers_per_server")
        parser.add_option("--time_per_job", dest="time_per_job",
                          action="store", type="int", default=None,
                          help="time_per_job")
        parser.add_option("--jobs_per_server", dest="jobs_per_server",
                          action="store", type="int", default=None,
                          help="jobs_per_server")
        parser.add_option("--instance_type", dest="instance_type",
                          action="store", type="string", default=None,
                          help="instance_type")
        parser.add_option("--domain", dest="domain",
                          action="store", type="string", default=None,
                          help="domain")
        parser.add_option("--dryrun", dest="dryrun", action="store_true",
                          default=False,
                          help="Don't actually stop or start any nodes.  Instead only display warnings for what operations "
                            "would be executed.")
        parser.add_option("--logfile", dest="log_file",
                          default=None,
                          help="File to append cluster stats as json object to")

    def execute(self, args):
        if not self.cfg.globals.enable_experimental:
            raise exception.ExperimentalFeature("The 'scalecluster' command")
        if len(args) != 1:
            self.parser.error("please specify a <cluster_tag>")
        cluster_tag = args[0]

        cluster = self.cm.get_cluster(cluster_tag)

        lb = alt.AltScaler(**self.specified_options_dict)
        lb.run(cluster)
