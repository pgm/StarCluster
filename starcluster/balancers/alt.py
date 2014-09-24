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
                 log_file=None,
                 max_to_initialize=5,
                 max_instances=10,
                 shutdown_allowed_after=45):
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
        self.max_to_initialize = max_to_initialize
        self.max_instances = max_instances
        self.shutdown_allowed_after = shutdown_allowed_after

    def get_unfulfilled_spot_requests(self, ec2):
        requests = ec2.get_all_spot_requests()
        open_requests = [r for r in requests if not (r.state in ['cancelled', 'closed', 'active'])]
        return open_requests

    def classify_instances(self, ec2, jobs_per_host, exclude, cluster_group):
        """
        #bucket all instances into one of three states:
        #    1. running, but not initialized
        #    2. initialized but not running any jobs
        #    3. initialized and running jobs
        """
        uninitialized = []
        idle = []
        active = []

        for instance in ec2.get_all_instances(filters={"instance.group-id": cluster_group.id}):
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
        lines = ssh.execute("/tmp/cluster_scripts/get_queue_stats_json")
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

        log.warn("Adding %d servers would give the best estimate of %s seconds to complete jobs", best_to_add, best_time)
        return best_to_add, best_time

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
        servers_to_add, estimated_time_to_complete = self.calc_number_of_servers_to_add(self.max_to_add, job_count, self.jobs_per_server,
                                                            running_servers, self.time_per_job,
                                                            self.time_to_add_servers_fixed, self.time_to_add_servers_per_server)
        unfulfilled_spot_requests = self.get_unfulfilled_spot_requests(cluster.ec2)

        log.warn("Estimated needed %d additional servers to complete job in %d seconds, currently %d open requests", servers_to_add, estimated_time_to_complete, len(unfulfilled_spot_requests))

        if (running_servers + servers_to_add) > self.max_instances:
            clamped_servers_to_add = max(0, min(self.max_instances - running_servers, servers_to_add))
            log.warn("%d servers are running, and adding %d would exceed our max of %d, so only adding %d" % (running_servers, servers_to_add, self.max_instances, clamped_servers_to_add))
            servers_to_add = clamped_servers_to_add

        cancel_count = 0
        spots_to_create = 0
        if servers_to_add > len(unfulfilled_spot_requests):
            log.warn("Creating spot requests: add_nodes(%s, spot_bid=%s, spot_only=True, instance_type=%s)", servers_to_add, self.spot_bid, self.instance_type)
            spots_to_create = servers_to_add - len(unfulfilled_spot_requests)
            if not self.dryrun:
                cluster.add_nodes(servers_to_add, spot_bid=self.spot_bid, spot_only=True, instance_type=self.instance_type)
        elif servers_to_add < len(unfulfilled_spot_requests):
            cancel_count = len(unfulfilled_spot_requests) - servers_to_add
            # cancel the last ones first, because they're less likely to be in the middle of being fulfilled
            #cluster.ec2.conn.cancel_spot_request(unfulfilled_spot_requests[-cancel_count:])
            request_ids = [request.id for request in unfulfilled_spot_requests[-cancel_count:]]
            cluster.ec2.conn.cancel_spot_instance_requests(request_ids)

        return {'servers_to_add': servers_to_add,
                'estimated_time_to_complete': estimated_time_to_complete,
                'unfulfilled_spot_requests': len(unfulfilled_spot_requests),
                'cancel_count': cancel_count,
                'spots_to_create': spots_to_create}

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
            # we assume the timezone is an offset of a multiple of hours, so the timezone doesn't actually matter
            up_time = now - iso8601.parse_date(instance.launch_time).replace(tzinfo=None)
            minutes_since_hour_start = (up_time.total_seconds()/60) % 60
            log.warn("%s has been up for %d minutes past hour boundary" % (instance, minutes_since_hour_start))

            if minutes_since_hour_start > min_minutes_into_hour:
                result.append(instance)
        return result

    def append_to_log(self, job_count, jobs_per_host, uninitialized, pending_uninitialized, idle, active, nodes_to_shutdown, scale_up_state):
        log.warn("uninitialized=%s, pending_uninitialized=%s, idle=%s, active=%s, nodes_to_shutdown=%s", uninitialized, pending_uninitialized, idle, active, nodes_to_shutdown)

        if self.log_file == None:
            return

        instances = []
        for l, t in ((uninitialized, "uninitialized"), (pending_uninitialized, "pending_uninitialized"), (idle, "idle"), (active, "active"),
                     (nodes_to_shutdown, "ready_to_shutdown")):
            for instance in l:
                alias = None
                if "alias" in instance.tags:
                    alias = instance.tags["alias"]
                running = 0
                if alias in jobs_per_host:
                    running = jobs_per_host[alias]
                instances.append({"alias": alias, "id": instance.id, "type": t, "running": running})

        now = datetime.datetime.now()
        with open(self.log_file, "a") as fd:
            record = dict(instances=instances, timestamp=now.isoformat(), scale_up_state=scale_up_state, job_count=job_count)
            fd.write(json.dumps(record))
            fd.write("\n")

    def poll_state(self, cluster):
        ec2 = cluster.ec2

        # loop until pending_uninitialized is empty
        while True:
            job_count, jobs_per_host = self.get_job_status(cluster.master_node.ssh)
            uninitialized, idle, active = self.classify_instances(cluster.ec2, jobs_per_host, [cluster.master_node.id], cluster.cluster_group)
            nodes_to_shutdown = self.only_those_near_hour_boundary(idle, min_minutes_into_hour=self.shutdown_allowed_after)

            # initializing nodes can take several minutes, so set a cap on how many we do before we re-inspect the state of the cluster
            pending_uninitialized = []
            if len(uninitialized) > self.max_to_initialize:
                pending_uninitialized = uninitialized[self.max_to_initialize:]
                uninitialized = uninitialized[:self.max_to_initialize]

            self.initialize_the_uninitialized(uninitialized, cluster)
            self.shutdown_the_idle(nodes_to_shutdown, cluster)
            scale_up_state = self.scale_up(job_count, len(active), cluster)
            self.heartbeat(ec2.region, ec2.aws_access_key_id, ec2.aws_secret_access_key, self.domain)

            self.append_to_log(job_count, jobs_per_host, uninitialized, pending_uninitialized, idle, active, nodes_to_shutdown, scale_up_state)

            if len(pending_uninitialized) == 0:
                break

    def run(self, cluster):
        self.poll_state(cluster)
