# frozen_string_literal: true

require_relative 'test_helper'


class TestWorker < Minitest::Test
  def test_worker_start
    ar, aw = IO.pipe # for app usage
    wr, ww = IO.pipe # for worker control
    worker = Coworker::Worker.new(ww) { ar.close; aw << "foobar\n"; aw.close; sleep 0.1 }

    assert_equal 1, worker.generation

    pid = worker.start
    sleep 0.05
    assert_nil Process.wait2(pid, Process::WNOHANG)

    msgs = 2.times.map { wr.gets(chomp: true) }.map { it.split(':') }.sort_by { it.first }

    pat = ['ping', pid.to_s, '1']
    assert_pattern {msgs[0] => pat }

    pat = ['reap', Integer]
    assert_pattern {msgs[1] => pat }

    sleep 0.05
    child_pid, status = Process.wait2(msgs[1][1].to_i)
    assert_equal msgs[1][1].to_i, child_pid
    assert_equal 0, status.exitstatus

    aw.close
    msg = ar.read
    assert_equal "foobar\n", msg
  ensure
    (Process.wait(pid) rescue nil) if pid

  end
end
