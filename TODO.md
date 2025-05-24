## implementation

- Continually select leader according to generation, spawn stamp  
  (earlier is preferable)
- Add warmup mechanism to tell supervisor worker is warmed up
- Remove leader state (plus leader thread) in worker, don't listen on
  connection.
- Move from socketpair to pipe for comms between supervisor and workers (since
  now comms is unidirectional.)
- Instead, to tell leader to spawn just send it a USR1 signal.

# Features

- make worker count a parameter
