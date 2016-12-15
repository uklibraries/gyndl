#!/bin/bash
TERM_CHILD=1 COUNT=8 QUEUES=gyndl bundle exec rake resque:workers
