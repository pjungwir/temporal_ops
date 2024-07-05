#!/bin/bash
set -eu

for scenario in full one; do
  echo
  echo "=================================="
  echo "           $scenario              "
  echo "=================================="

  for impl in pj1 pj2 boris2 boris3 boris3b; do
    echo
    echo "           $impl              "
    echo "------------------------------"
    pgbench -f $scenario-$impl.sql --transactions=100 temporal_ops
  done;
done | tee bench.log;
