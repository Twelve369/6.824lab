#!/usr/bin/env bash

sleep 15 &
sleep 9 &
sleep 6 &
wait -n
echo “First job has been completed.”
wait -n
echo “Next job has been completed.”
wait -n
echo “All jobs have been completed.”