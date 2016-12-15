#!/bin/bash
TERM_CHILD=1 COUNT=1 QUEUES=gyndl_solr bundle exec rake resque:workers
#TERM_CHILD=1 COUNT=1 QUEUES=gyndl bundle exec rake resque:workers
