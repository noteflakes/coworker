# frozen_string_literal: true

module Coworker
  class Worker
    attr_reader :generation

    def initialize(conn, generation = 1, &app)
      @fd = conn.fileno
      @app = app
      @generation = generation
    end

    # Starts a forked orphaned worker process, 
    def start
      r, w = IO.pipe
      w.sync = true

      # we count on ChildSubreaper to make it so the
      # grandparent process reaps the workers
      fork do
        fork do
          w << "#{Process.pid}"
          w.close
          run
        end

        w.close
        send_msg("reap:#{Process.pid}\n")
      end
      # get grand-child PID
      w.close
      r.read(16).to_i
    end

    private

    def send_msg(msg)
      if !@conn
        @conn = IO.new(@fd)
        @conn.sync = true
      end
      # tell supervisor to reap child
      @conn.write "reap:#{Process.pid}\n"
    end

    def on_start
    end

    def on_stop
    end

    def run
      on_start

      @spawn_requests = Queue.new
      trap(:SIGUSR1) { @spawn_requests << true }
      pid = Process.pid
      spawn_thread = Thread.new { spawn_loop}
      pinger_thread = Thread.new { ping_loop }
      @app.call(self)
    ensure
      on_stop
      spawn_thread.kill.join
      pinger_thread.kill.join
      @conn.close
    end

    def start_next_generation
      self.class.new(@conn, @generation + 1, &@app).start
    end

    def spawn_loop
      loop do
        @spawn_requests.shift && start_next_generation
      end
    end

    def ping_loop
      @pid ||= Process.pid
      loop do
        send_msg("ping:#{@pid}:#{@generation}\n")
        sleep 5
      end
    end
  end
end
