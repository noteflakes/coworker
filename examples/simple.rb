# frozen_string_literal

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'coworker', path: '..'
end

app = ->(w) do
  loop do
    puts "#{Time.now} pid: #{Process.pid} generation: #{w.generation}"
    sleep 5
  end
end

supervisor = Coworker::Supervisor.new(&app)
supervisor.run
