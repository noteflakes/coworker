# frozen_string_literal: true

module Coworker
  class Worker
    attr_reader :generation

    def initialize(conn, generation = 1, &app)
      @fd = conn.fileno
      @app = app
      @generation = generation
    end

    def next_generation
      self.class.new(IO.new(@fd), @generation + 1, &@app)
    end

    # Starts a forked orphaned worker process, 
    def start
      r, w = IO.pipe
      w.sync = true

      # we count on ChildSubreaper to make it so the
      # grandparent process reaps the workers
      parent_pid = fork do
        fork do
          send_ping
          w << "#{Process.pid}"
          w.close
          run
        end

        w.close
        send_msg("reap:#{Process.pid}")
      end
      # get grand-child PID
      w.close
      Process.wait(parent_pid)
      r.read(16).to_i
    end

    private

    def send_msg(msg)
      if !@conn
        @conn = IO.new(@fd)
        @conn.sync = true
      end
      # tell supervisor to reap child
      @conn.write "#{msg}\n"
    end

    def on_start
    end

    def on_stop
    end

    def run
      on_start

      @spawn_requests = Queue.new
      trap(:SIGUSR1) { @spawn_requests << true }

      spawn_thread = Thread.new { spawn_loop}
      pinger_thread = Thread.new { ping_loop }
      
      @app.call(self)
    ensure
      on_stop
      spawn_thread.kill.join
      pinger_thread.kill.join
      @conn&.close rescue nil
    end

    def spawn_loop
      loop do
        @spawn_requests.shift && next_generation.start
      end
    end

    def send_ping
      @pid ||= Process.pid
      send_msg("ping:#{@pid}:#{@generation}")
    end

    def ping_loop
      loop do
        sleep 5
        send_ping
      end
    end
  end
end
