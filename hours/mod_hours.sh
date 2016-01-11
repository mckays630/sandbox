#!/bin/bash
cut -d, -f5,6 $1 | ./hours.pl 
