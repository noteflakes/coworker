# frozen_string_literal: true
 
module Coworker
  CLUSTER_SIZE = 2

  class Supervisor
    def initialize(**opts, &app)
      @opts = opts
      @app = app

      @leader_pid = nil
      @workers = {}
      @messages = Queue.new
      @socket_supervisor, @socket_worker = UNIXSocket.socketpair(:SOCK_SEQPACKET)
    end

    def run
      ChildSubreaper.enable

      p supervisor_pid: Process.pid
      trap('SIGUSR1') { @messages.push 'respawn' }
      start_message_parser
      while true
        sleep 1
        check_liveness
        if need_spawn?
          pid = spawn_worker
          @workers[pid] = { stamp: Time.now.to_i, generation: 1 } if pid
        end
        select_leader if !@leader_pid
      end
    rescue Interrupt
      return
    ensure
      stop_message_parser
      kill_all_workers
    end

    private

    def start_message_parser
      @message_parser_thread = Thread.new { recv_msg_loop }
    end

    def stop_message_parser
      @message_parser_thread.kill
      @message_parser_thread.join
    end

    def recv_msg_loop
      loop do
        msg = @socket_supervisor.gets(chomp: true)
        @messages.push msg
      end
    end

    def send_msg(msg)
      @socket_supervisor.write("#{msg}\n")
    end

    WORKER_PING_TTL = 15

    def check_liveness
      now = Time.now.to_i
      while !@messages.empty?
        msg = @messages.shift
        p msg: msg
        case msg
        when /ping:(\d+):(\d+)/
          m = Regexp.last_match
          pid = m[1].to_i
          generation = m[2].to_i
          @workers[pid] = { stamp: now, generation: generation }
        when /reap:(\d+)/
          m = Regexp.last_match
          pid = m[1].to_i
          reap_process(pid)
        when 'respawn'
          respawn
        end
      end

      # check unresponsive workers
      deadline = now - WORKER_PING_TTL
      unresponsive_pids = @workers.each_with_object([]) { |(k, v), o| o << k if v[:stamp] < deadline }
      unresponsive_pids.each do |pid|
        kill_process(pid)
        @workers.delete(pid)
        select_leader if @leader_pid == pid
      end
    end

    def kill_process(pid, timeout = 10)
      t0 = Time.now
      Process.kill('SIGTERM', pid)
      while Time.now - t0 < timeout
        wpid, _status = Process.wait2(pid, Process::WNOHANG)
        return if wpid == pid

        sleep 0.25
      end

      Process.kill('SIGKILL', pid)
      Process.wait(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      # ignore
    end

    def reap_process(pid, timeout = 10)
      p reap: pid
      t0 = Time.now
      while Time.now - t0 < timeout
        wpid, _status = Process.wait2(pid, Process::WNOHANG)
        return if wpid == pid

        sleep 0.25
      end

      Process.kill('SIGKILL', pid)
      Process.wait(pid)    
    rescue Errno::ESRCH, Errno::ECHILD
      # ignore
    end

    def kill_all_workers(timeout = 10)
      t0 = Time.now
      puts "Terminating all workers..."
      pids = @workers.keys
      pids.each { Process.kill('SIGTERM', it) rescue nil }

      while Time.now - t0 < timeout
        wpid, _status = Process.wait2(-1, Process::WNOHANG)
        if wpid
          p reap: wpid
          pids.delete(wpid)
          return if pids.empty?
        else
          sleep 0.25
        end
      end

      pids.each do
        Process.kill('SIGKILL', it) rescue nil
        Process.wait(it)
      end
    rescue Errno::ECHILD
      # ignore
    end

    def need_spawn?
      @workers.size < CLUSTER_SIZE
    end

    def select_leader
      @leader_pid = @workers.keys.sort_by { @workers[it][:generation] }.last
      p select_leader: @leader_pid
      Process.kill('SIGUSR1', @leader_pid)
      @leader_pid
    end

    def spawn_worker
      if @leader_pid
        send_msg('spawn')
        return nil
      end

      Worker.new(@socket_worker, &@app).start
    end

    def respawn
      p respawn: 1
      return if !@leader_pid

      p respawn: 2
      pids_to_replace = @workers.keys.select { it != @leader_pid }

      p respawn: 3, pids_to_replace: pids_to_replace
      while (pid = pids_to_replace.shift)
        p respawn: 4, pid: pid
        size_before = @workers.size
        send_msg('spawn')
        # Process.kill('SIGUSR1', @leader_pid)
        sleep 0.1 while @workers.size > size_before
        kill_process(pid)
      end

      new_leader = select_leader
      p respawn: 5, new_leader: new_leader
    end
  end
end
