__author__ = 'pmontgom'

import json
import socket
import boto.sdb
import math
import time
from starcluster.logger import log
import datetime
import iso8601

class AltScaler:
    def __init__(self,
                 spot_bid=0.01,
                 max_to_add=5,
                 time_per_job=30 * 60,
                 time_to_add_servers_fixed=60,
                 time_to_add_servers_per_server=30,
                 instance_type="m3.medium",
                 domain="cluster-deadmans-switch",
                 dryrun=False,
                 jobs_per_server=1,
                 log_file=None):
        self.max_to_add = max_to_add
        self.time_per_job = time_per_job
        self.time_to_add_servers_fixed = time_to_add_servers_fixed
        self.time_to_add_servers_per_server = time_to_add_servers_per_server
        self.instance_type = instance_type
        self.spot_bid = spot_bid
        self.domain = domain
        self.dryrun = dryrun
        self.jobs_per_server = jobs_per_server
        self.log_file = log_file

    def get_unfulfilled_spot_requests(self, ec2):
        requests = ec2.get_all_spot_requests()
        open_requests = [r for r in requests if not (r.state in ['cancelled', 'closed', 'active'])]
        return open_requests

    def classify_instances(self, ec2, jobs_per_host, exclude):
        """
        #bucket all instances into one of three states:
        #    1. running, but not initialized
        #    2. initialized but not running any jobs
        #    3. initialized and running jobs
        """
        uninitialized = []
        idle = []
        active = []

        for instance in ec2.get_all_instances():
                if instance.state in ['stopped', 'terminated']:
                    continue

                if instance.id in exclude:
                    continue

                alias = None
                if 'alias' in instance.tags:
                    alias = instance.tags['alias']

                if not ("initialized" in instance.tags):
                    uninitialized.append(instance)
                    continue

                if alias in jobs_per_host and jobs_per_host[alias] > 0:
                    active.append(instance)
                else:
                    idle.append(instance)

        return (uninitialized, idle, active)

    def get_job_stats(self, ssh):
        lines = ssh.execute("/xchip/scripts/get_queue_stats_json")
        stats = json.loads("".join(lines))
        return stats


    def estimate_completion_time(self, job_count, jobs_per_server, running_servers, servers_to_add, time_per_job, time_to_add_servers_fixed, time_to_add_servers_per_server):
        servers_needed = job_count/jobs_per_server

        if servers_needed == 0:
            return 0

        # if nothing is running, report essentially inf
        if (running_servers + servers_to_add) == 0:
            return 1e30

        # the additional time required to bring new servers online
        time_to_bring_up_servers = time_to_add_servers_fixed + servers_to_add * time_to_add_servers_per_server

        # if we have fewer jobs then servers, then assume we're half way through them and it will take
        # the same amount of time to complete those jobs, regardless of how many additional servers we bring on line.
        time = time_per_job / 2

        # now, how many jobs are left, after the ones running on the servers are done?
        servers_needed = max(servers_needed - running_servers, 0)

        # For the remaining, assume that we have to wait for all new nodes to come online before the next batch all runs.
        # this is a clear over-estimate of the time needed, but keeps things simple.
        time += time_to_bring_up_servers + math.ceil(servers_needed / float(running_servers + servers_to_add)) * time_per_job

        return time

    def calc_number_of_servers_to_add(self, max_to_add, job_count, jobs_per_server, running_servers, time_per_job, time_to_add_servers_fixed, time_to_add_servers_per_server):
        best_time = None
        best_to_add = None

        for servers_to_add in xrange(max_to_add+1):
            time = self.estimate_completion_time(job_count, jobs_per_server, running_servers, servers_to_add, time_per_job, time_to_add_servers_fixed, time_to_add_servers_per_server)
            if best_time == None or best_time > time:
                best_to_add = servers_to_add
                best_time = time

        log.debug("Adding %d servers would give the best estimate of %s seconds to complete jobs", best_to_add, best_time)
        return best_to_add

    def initialize_the_uninitialized(self, uninitialized, cluster):
        for instance in uninitialized:
            alias = instance.tags['alias']

            if self.dryrun:
                log.warn("initializing node: add_nodes(%s, no_create=True)", alias)
            else:
                cluster.add_nodes(1, aliases=[alias], no_create=True)
                assert "initialized" in cluster.get_node(alias).tags

    def shutdown_the_idle(self, idle, cluster):
        # would be faster to just terminate the instance.  Is there any problem with that?
        # trying termination first.  One of the problems with clean stopping is if the node initialization failed
        # then it won't be able to remove it either.

        instance_ids = [instance.id for instance in idle]
        log.warn("remove_nodes(%s)", instance_ids)
        if not self.dryrun:
            cluster.ec2.terminate_instances(instance_ids)

        #for instance in idle:
        #    aliases = [instance.tags['alias']]
        #    nodes = cluster.get_nodes(aliases)
        #
        #    if self.dryrun:
        #        log.warn("remove_nodes(%s)", nodes)
        #    else:
        #        cluster.remove_nodes(nodes)

    def scale_up(self, job_count, running_servers, cluster):
        servers_to_add = self.calc_number_of_servers_to_add(self.max_to_add, job_count, self.jobs_per_server,
                                                            running_servers, self.time_per_job,
                                                            self.time_to_add_servers_fixed, self.time_to_add_servers_per_server)
        unfulfilled_spot_requests = self.get_unfulfilled_spot_requests(cluster.ec2)
        log.warn("Estimated needed %d additional servers, currently %d open requests", servers_to_add, len(unfulfilled_spot_requests))
        if servers_to_add > len(unfulfilled_spot_requests):
            log.warn("Creating spot requests: add_nodes(%s, spot_bid=%s, spot_only=True, instance_type=%s)", servers_to_add, self.spot_bid, self.instance_type)
            if not self.dryrun:
                cluster.add_nodes(servers_to_add, spot_bid=self.spot_bid, spot_only=True, instance_type=self.instance_type)
        elif servers_to_add < len(unfulfilled_spot_requests):
            cancel_count = len(unfulfilled_spot_requests) - servers_to_add
            # cancel the last ones first, because they're less likely to be in the middle of being fulfilled
            #cluster.ec2.conn.cancel_spot_request(unfulfilled_spot_requests[-cancel_count:])
            cluster.ec2.conn.cancel_spot_instance_requests(unfulfilled_spot_requests[-cancel_count:])

    def heartbeat(self, region, key, secret, domain):
        sdbc = boto.sdb.connect_to_region(region.name,
           aws_access_key_id=key,
           aws_secret_access_key=secret)

        assert sdbc != None

        dom = sdbc.get_domain(domain, validate=False)
        dom.put_attributes('heartbeat', {'timestamp': time.time(), 'hostname': socket.getfqdn()})

    def get_job_status(self, ssh):
        job_stats = self.get_job_stats(ssh)
        log.debug("job_stats=%s", job_stats)

        job_count = job_stats["running_count"] + job_stats["waiting_count"]
        jobs_per_host = {}
        for host in job_stats['hosts']:
            jobs_per_host[host['name']] = host['running_jobs']

        return job_count, jobs_per_host

    def only_those_near_hour_boundary(self, instances, min_minutes_into_hour):
        result = []
        now = datetime.datetime.now()
        for instance in instances:
            uptime = now - iso8601.parse_date(instance.launch_time)
            uptime_since_hour_start = uptime.total_seconds() % (60*60)

            if uptime_since_hour_start > min_minutes_into_hour:
                result.append(instance)
        return result

    def append_to_log(self, job_count, jobs_per_host, uninitialized, idle, active, nodes_to_shutdown):
        log.warn("uninitialized=%s, idle=%s, active=%s, nodes_to_shutdown=%s", uninitialized, idle, active, nodes_to_shutdown)

        if self.log_file == None:
            return

        instances = []
        for l, t in ((uninitialized, "uninitialized"), (idle, "idle"), (active, "active"), ("ready_to_shutdown", nodes_to_shutdown)):
            for instance in l:
                alias = None
                if "alias" in instance.tags:
                    alias = instance.tags["alias"]
                instances.append({"alias": alias, "id": l.id, "type": t})

        now = datetime.datetime.now()
        with open(self.log_file, "a") as fd:
            fd.write(json.dumps(dict(instances=instances, timestamp=now.isoformat())))
            fd.write("\n")

    def poll_state(self, cluster):
        ec2 = cluster.ec2

        job_count, jobs_per_host = self.get_job_status(cluster.master_node.ssh)
        uninitialized, idle, active = self.classify_instances(cluster.ec2, jobs_per_host, [cluster.master_node.id])
        nodes_to_shutdown = self.only_those_near_hour_boundary(idle, min_minutes_into_hour=45)

        self.append_to_log(job_count, jobs_per_host, uninitialized, idle, active, nodes_to_shutdown)

        self.initialize_the_uninitialized(uninitialized, cluster)
        self.shutdown_the_idle(idle, cluster)
        self.scale_up(job_count, len(active), cluster)
        self.heartbeat(ec2.region, ec2.aws_access_key_id, ec2.aws_secret_access_key, self.domain)

    def run(self, cluster):
        self.poll_state(cluster)
