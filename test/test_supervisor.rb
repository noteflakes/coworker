# frozen_string_literal: true

require_relative 'test_helper'

class TestSupervisor < Minitest::Test
  class TestSupervisor < Coworker::Supervisor
    attr_reader :messages, :workers, :leader_pid

    def select_leader
      super
    end
  end

  class TestWorker < Coworker::Worker
    attr_reader :fd
  end

  def test_supervisor_check_liveness
    ar, aw = IO.pipe
    aw.sync = true

    app = -> do
      sleep 10
    rescue SignalException
      aw.write("terminated:#{Process.pid}\n")
      aw.close
    end
    s = TestSupervisor.new(&app)

    pids = 3.times.map { fork(&app) }
    t0 = Time.now
    s.messages << "ping:#{pids[0]}:1"
    s.messages << "ping:#{pids[1]}:2"
    s.messages << "ping:#{pids[2]}:3"
    s.check_liveness


    assert_equal 3, s.workers.size
    assert_equal({ stamp: t0.to_i, generation: 1 }, s.workers[pids[0]])
    assert_equal({ stamp: t0.to_i, generation: 2 }, s.workers[pids[1]])
    assert_equal({ stamp: t0.to_i, generation: 3 }, s.workers[pids[2]])

    # check removal of unredsponsive worker
    s.workers[pids[1]][:stamp] = t0.to_i - 60
    s.check_liveness

    assert_nil s.workers[pids[1]]
    msg = ar.gets(chomp: true)
    assert_equal "terminated:#{pids[1]}", msg
  ensure
    s.stop
    pids&.each do
      Process.kill('SIGKILL', it)
      Process.wait(it)
    rescue SystemCallError
      # ignore
    end
  end

  def test_supervisor_select_leader
    app = -> { sleep 10 }
    s = TestSupervisor.new(&app)

    pids = 3.times.map { fork(&app) }
    t0 = Time.now
    s.messages << "ping:#{pids[0]}:1"
    s.messages << "ping:#{pids[1]}:2"
    s.messages << "ping:#{pids[2]}:3"
    
    s.check_liveness
    ret = s.select_leader

    assert_equal pids[2], ret
    assert_equal pids[2], s.leader_pid
  ensure
    s.stop
    pids&.each do
      Process.kill('SIGKILL', it)
      Process.wait(it)
    rescue SystemCallError
      # ignore
    end
  end

  def test_supervisor_select_leader_again
    app = -> { sleep 10 }
    s = TestSupervisor.new(&app)

    pids = 3.times.map { fork(&app) }
    t0 = Time.now
    s.messages << "ping:#{pids[0]}:1"
    s.messages << "ping:#{pids[1]}:2"
    s.messages << "ping:#{pids[2]}:3"
    
    s.check_liveness
    s.select_leader

    assert_equal pids[2], s.leader_pid

    s.workers[pids[2]][:stamp] = t0.to_i - 300

    s.check_liveness
    s.select_leader

    assert_nil s.workers[pids[2]]
    assert_equal pids[1], s.leader_pid
  ensure
    s.stop
    pids&.each do
      Process.kill('SIGKILL', it)
      Process.wait(it)
    rescue SystemCallError
      # ignore
    end
  end
end
