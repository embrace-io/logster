from logster.logster_helper import LogsterOutput
import socket


class DogstatsdOutput(LogsterOutput):
    shortname = 'dogstatsd'


    @classmethod
    def add_options(cls, parser):
        parser.add_option('--dogstatsd-host', action='store',
                          help='Hostname and port for dogstatsd collector, e.g. statsd.example.com:8125')


    def __init__(self, parser, options, logger):
        super(DogstatsdOutput, self).__init__(parser, options, logger)
        if not options.dogstatsd_host:
            parser.print_help()
            parser.error("You must supply --dogstatsd-host when using 'dogstatsd' as an output type.")
        self.dogstatsd_host = options.dogstatsd_host


    def submit(self, metrics):
        if (not self.dry_run):
            host = self.dogstatsd_host.split(':')
            host[0] = socket.gethostbyname(host[0])

        for metric in metrics:
            metric_name = self.get_metric_name(metric)

            # parse dogstatsd tags
            metric_tags = ''

            if len(metric.tags) > 0:
                metric_tags = ','.join(metric.tags)
                # strip anywhite space
                metric_tags = ''.join(metric_tags.split())
                self.logger.debug("Dogstatsd tags: {}".format(metric_tags))
                metric_tags = '|#' + metric_tags

            metric_string = "%s:%s|%s%s" % (metric_name, metric.value, metric.metric_type, metric_tags)

            self.logger.debug("Submitting dogstatsd metric: %s" % metric_string)

            if (not self.dry_run):
                udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                udp_sock.sendto(metric_string.encode('ascii'), (host[0], int(host[1])))
            else:
                print("%s %s" % (self.dogstatsd_host, metric_string))
