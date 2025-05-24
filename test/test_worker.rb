# frozen_string_literal: true

require_relative 'test_helper'


class TestWorker < Minitest::Test
  def test_worker_start
    wr, ww = IO.pipe # for worker control
    ar, aw = IO.pipe # for app usage
    worker = Coworker::Worker.new(ww) { |w| ar.close; aw << "foobar\n"; aw.close; sleep 0.1 }

    assert_equal 1, worker.generation

    pid = worker.start
    sleep 0.05
    assert_nil Process.wait2(pid, Process::WNOHANG)

    msgs = 2.times.map { wr.gets(chomp: true) }.map { it.split(':') }.sort_by { it.first }

    assert_equal ['ping', pid.to_s, '1'], msgs[0]
    assert_equal 'reap', msgs[1][0]

    parent_pid = msgs[1][1].to_i

    sleep 0.05
    # worker parent will have already been reaped
    assert_raises(Errno::ECHILD) { Process.wait2(parent_pid) }

    aw.close
    msg = ar.read
    assert_equal "foobar\n", msg
  ensure
    (Process.wait(pid) rescue nil) if pid
  end

  def test_next_generation
    _wr, ww = IO.pipe
    worker = Coworker::Worker.new(ww) { sleep 1 }
    
    assert_equal 1, worker.generation

    n = worker.next_generation
    assert_kind_of Coworker::Worker, n
    assert_equal 2, n.generation
  end

  def test_usr1_signal
    wr, ww = IO.pipe
    worker = Coworker::Worker.new(ww) { sleep 0.2 }

    pid1 = worker.start
    sleep 0.05

    ww.close

    # consume reap/ping messages from forked process
    2.times { wr.gets }

    Process.kill('SIGUSR1', pid1)
    sleep 0.05

    msgs = 2.times.map { wr.gets(chomp: true) }.map { it.split(':') }.sort_by { it.first }

    assert_equal 'ping', msgs[0][0]
    pid2 = msgs[0][1].to_i
    assert_equal '2', msgs[0][2]

    assert_equal 'reap', msgs[1][0]

    assert_nil Process.wait2(pid2, Process::WNOHANG)
  ensure
    (Process.wait(pid1) rescue nil) if pid1
    (Process.wait(pid2) rescue nil) if pid2
  end
end
