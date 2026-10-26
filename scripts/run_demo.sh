#!/usr/bin/env bash
# Runs all three lab scenarios and saves the terminal output to evidence/.
# Prereqs: RabbitMQ running (docker compose up -d) and the venv activated.
set -u
cd "$(dirname "$0")/.."
mkdir -p evidence
PY="python3 -u"

reset_state() {
  docker exec rabbitmq rabbitmqctl purge_queue orders >/dev/null 2>&1 || true
  : > processed_events.txt
}

start_worker() {   # $1=name  $2=logfile  rest=extra args
  local name=$1 logfile=$2; shift 2
  $PY worker.py --name "$name" "$@" > "$logfile" 2>&1 &
  echo $!
}

stop_workers() { kill "$@" 2>/dev/null; wait "$@" 2>/dev/null; }

# Make sure the queue exists before purging
$PY producer.py --count 0 >/dev/null 2>&1

echo "=== Scenario 1: successful processing + two competing consumers ==="
reset_state
W1=$(start_worker worker-1 evidence/1_worker-1.log)
W2=$(start_worker worker-2 evidence/1_worker-2.log)
sleep 2
$PY producer.py --count 10 2>&1 | tee evidence/1_producer.log
sleep 8
stop_workers "$W1" "$W2"
{
  echo "--- worker-1 ---"; cat evidence/1_worker-1.log
  echo "--- worker-2 ---"; cat evidence/1_worker-2.log
  echo "--- who processed what ---"
  grep -h "PROCESSED" evidence/1_worker-*.log | sort
} | tee evidence/1_summary.log

echo; echo "=== Scenario 2: worker crashes before ACK -> redelivery ==="
reset_state
W1=$(start_worker worker-1 evidence/2_worker-1.log --crash-after 2)
W2=$(start_worker worker-2 evidence/2_worker-2.log)
sleep 2
$PY producer.py --count 6 2>&1 | tee evidence/2_producer.log
sleep 10
echo "worker-1 still alive? $(kill -0 "$W1" 2>/dev/null && echo yes || echo 'no (crashed as planned)')" | tee evidence/2_status.log
echo "worker-2 still alive? $(kill -0 "$W2" 2>/dev/null && echo yes || echo no)" | tee -a evidence/2_status.log
docker exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged 2>/dev/null | tee -a evidence/2_status.log
stop_workers "$W1" "$W2"
{
  echo "--- worker-1 ---"; cat evidence/2_worker-1.log
  echo "--- worker-2 ---"; cat evidence/2_worker-2.log
} | tee evidence/2_summary.log

echo; echo "=== Scenario 3a: producer re-sends the same event_id -> duplicate detected ==="
reset_state
W1=$(start_worker worker-1 evidence/3a_worker-1.log)
W2=$(start_worker worker-2 evidence/3a_worker-2.log)
sleep 2
$PY producer.py --count 3 --duplicate 2 2>&1 | tee evidence/3a_producer.log
sleep 6
stop_workers "$W1" "$W2"
{
  echo "--- worker-1 ---"; cat evidence/3a_worker-1.log
  echo "--- worker-2 ---"; cat evidence/3a_worker-2.log
  echo "--- processed_events.txt ---"; cat processed_events.txt
} | tee evidence/3a_summary.log

echo; echo "=== Scenario 3b: crash AFTER processing but BEFORE ACK -> redelivered duplicate ==="
reset_state
W1=$(start_worker worker-1 evidence/3b_worker-1.log --crash-after 1 --crash-stage after-process)
sleep 1
W2=$(start_worker worker-2 evidence/3b_worker-2.log)
sleep 2
$PY producer.py --count 4 2>&1 | tee evidence/3b_producer.log
sleep 8
stop_workers "$W1" "$W2"
{
  echo "--- worker-1 ---"; cat evidence/3b_worker-1.log
  echo "--- worker-2 ---"; cat evidence/3b_worker-2.log
} | tee evidence/3b_summary.log

echo; echo "Done. Evidence saved in evidence/"
