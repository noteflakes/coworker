# frozen_string_literal: true

require_relative './worker'

module Coworker
  CLUSTER_SIZE = 2

  class Supervisor
    def initialize(**opts, &app)
      @opts = opts
      @app = app

      @leader_pid = nil
      @workers = {}
      @messages = Queue.new
      @pipe_r, @pipe_w = IO.pipe
      # @socket_supervisor, @socket_worker = UNIXSocket.socketpair(:SOCK_SEQPACKET)
    end

    def run
      ChildSubreaper.enable

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

    def stop
      kill_all_workers
    end

    def check_liveness
      process_received_messages
      reap_unresponsive_workers
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
      @pipe_w.close
      loop do
        msg = recv_msg
        @messages.push msg
      end
    end

    def recv_msg
      @pipe_r.gets(chomp: true)
    end

    WORKER_PING_TTL = 15

    def process_received_messages
      while !@messages.empty?
        msg = @messages.shift
        parts = msg.split(':')
        case parts[0]
        when 'ping'
          handle_ping(*parts[1..2].map(&:to_i))
        when 'reap'
          handle_reap(parts[1].to_i)
        when 'respawn'
          handle_respawn
        end
      end      
    end

    def handle_ping(pid, generation)
      @workers[pid] = { stamp: Time.now.to_i, generation: generation }
    end

    def handle_reap(pid)
      reap_process(pid)
    end

    def handle_respawn
      respawn
    end

    def reap_unresponsive_workers
      deadline = Time.now.to_i - WORKER_PING_TTL
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
      pids = @workers.keys
      pids.each { Process.kill('SIGTERM', it) rescue nil }

      while Time.now - t0 < timeout
        wpid, _status = Process.wait2(-1, Process::WNOHANG)
        if wpid
          pids.delete(wpid)
          @workers.delete(wpid)
          return if pids.empty?
        else
          sleep 0.25
        end
      end

      pids.each do
        Process.kill('SIGKILL', it) rescue nil
        Process.wait(it)
        @workers.delete(it)
      end
    rescue Errno::ECHILD
      # ignore
    end

    def need_spawn?
      @workers.size < CLUSTER_SIZE
    end

    def select_leader
      @leader_pid = @workers.keys.sort_by { @workers[it][:generation] }.last
    end

    # returns [void]
    def spawn_worker
      return send_signal('SIGUSR1', @leader_pid) if @leader_pid
        
      worker_class.new(@pipe_w, &@app).start
    end

    def worker_class
      @opts[:worker_class] ||= Worker
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
        send_signal('SIGUSR1', @leader_pid)
        sleep 0.1 while @workers.size > size_before
        kill_process(pid)
      end

      new_leader = select_leader
      p respawn: 5, new_leader: new_leader
    end

    def send_signal(sig, pid)
      Process.kill(sig, pid)
    end
  end
end
